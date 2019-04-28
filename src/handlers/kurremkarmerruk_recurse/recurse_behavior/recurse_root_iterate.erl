% ------------------------------------------------------------------------------
%
% Copyright © 2018-2019, Lauri Moisio <l@arv.io>
%
% The ISC License
%
% Permission to use, copy, modify, and/or distribute this software for any
% purpose with or without fee is hereby granted, provided that the above
% copyright notice and this permission notice appear in all copies.
%
% THE SOFTWARE IS PROVIDED “AS IS” AND THE AUTHOR DISCLAIMS ALL WARRANTIES
% WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
% MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
% ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
% WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
% ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
% OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
%
% ------------------------------------------------------------------------------
%
-module(recurse_root_iterate).

-behavior(recurse_behavior).
-export([init/4,response/5,reroute/5,stop/2]).

-include_lib("kernel/include/logger.hrl").
-include_lib("dnslib/include/dnslib.hrl").
-include_lib("dnslib/include/dnsmsg.hrl").
-include_lib("kurremkarmerruk/include/recurse.hrl").

-ifdef(EUNIT).
-include_lib("eunit/include/eunit.hrl").
-endif.

-import(kurremkarmerruk_recurse_address_reputation, [
    referral_choose_address/2,
    referral_choose_address/3
]).
-import(kurremkarmerruk_recurse_resolve, [
    route_question/5,
    route_question/6
]).


init(Questions, true, UseAf, Opts0) ->
    % Set a time limit for the process...
    Opts1 = case proplists:get_value(time_limit, Opts0) of
        undefined -> Opts0;
        Limit -> [{deadline, erlang:monotonic_time(millisecond) + Limit}|Opts0]
    end,
    Opts2 = [{recurse_id, make_ref()}|Opts1],
    {ok, init_questions(Questions, UseAf, Opts2, []), Opts2}.


init_questions([], _, _, Acc) ->
    Acc;
init_questions([OrigQuestion|Rest], UseAf, Opts, Acc) ->
    [{SendQuestion, _, _}=Referral|_] = Chain = init_referral(OrigQuestion, UseAf, Opts, []),
    ActionType = case OrigQuestion of
        {_, referral, _} -> route_question_skip_precheck;
        {_, addressless_referral, _} -> route_question_skip_precheck;
        _ -> route_question
    end,
    Tuple = case referral_choose_address(Referral, UseAf) of
        {ok, Address, Timeout} -> {ActionType, {#{}, SendQuestion}, {basic, {Address, 53}}, Timeout, Chain, #tried{}};
        {error, no_suitable_address} -> {close_question, {OrigQuestion, {error, no_reroute_address}}};
        {addressless_nsrrs, NsRrs} ->
            {ok, CaseTuple} = nsrrs_to_next_query(NsRrs, UseAf, Opts, OrigQuestion, {OrigQuestion, addressless_referral, NsRrs}, Chain),
            CaseTuple
    end,
    init_questions(Rest, UseAf, Opts, [Tuple|Acc]).


init_referral(Entry, UseAf, Opts, Acc) ->
    case Entry of
        {Question, _, _}=Referral when is_tuple(Question) -> ok;
        Question ->
            Namespace = proplists:get_value(namespace, Opts),
            case closest_referral(Question, Namespace) of
                {ok, Referral} -> ok
            end
    end,
    case Referral of
        {_, referral, _} -> [Referral|Acc];
        {_, addressless_referral, [NsRr|_]} ->
            {_, _, _, _, Domain} = NsRr,
            Type = case hd(UseAf) of
                inet  -> a;
                inet6 -> aaaa
            end,
            init_referral({Domain, Type, in}, UseAf, Opts, [Referral|Acc])
    end.


% Should we provide a reason for reroute? What about error handling?
reroute(Questions, true, Tried, UseAf, Opts) ->
    case proplists:get_value(deadline, Opts) of
        undefined -> {ok, reroute_questions(Questions, UseAf, Tried, Opts, []), Opts}
    end.


reroute_questions([], _, _, _, Acc) ->
    Acc;
reroute_questions([{Question, [Referral|Chain]}|Rest], UseAf, Tried, Opts, Acc) when ?IS_REFERRAL(Referral) ->
    Tuple = case element(2, Referral) of
        addressless_referral -> % Referral could only be addressless if we previously returned one from here...
            OrigQuestion = element(1, Referral),
            NsDomain = dnslib:normalize_domain(?RESOURCE_DOMAIN(Question)),
            case [GenNsRr || GenNsRr <- element(3, Referral), dnslib:normalize_domain(?RESOURCE_DATA(GenNsRr)) =/= NsDomain] of
                [] -> {close_question, {OrigQuestion, {error, no_reroute_address}}};
                NotTried ->
                    {ok, CaseTuple} = nsrrs_to_next_query(NotTried, UseAf, Opts, OrigQuestion, {OrigQuestion, addressless_referral, NotTried}, Chain),
                    CaseTuple
            end;
        referral ->
            case referral_choose_address(Referral, UseAf, Tried) of
                {ok, Address, Timeout} -> route_question(#{}, Question, {basic, {Address, 53}}, Timeout, [Referral|Chain], Tried);
                {error, no_suitable_address} -> {close_question, {Question, {error, no_reroute_address}}};
                {addressless_nsrrs, NsRrs} ->
                    {ok, CaseTuple} = nsrrs_to_next_query(NsRrs, UseAf, Opts, Question, {Question, addressless_referral, NsRrs}, Chain),
                    CaseTuple
            end
    end,
    reroute_questions(Rest, UseAf, Tried, Opts, [Tuple|Acc]).


response(Answers, Res, UseAf, Tried, Opts) ->
    % Handle answers, and then transform any route_question hubbub
    % into close_question tuples if deadline has been passed
    Actions = response_handle_answers(Answers, Res, UseAf, Tried, Opts, []),
    case proplists:get_value(deadline, Opts) of
        undefined -> {ok, Actions, Opts}
    end.


stop(_Answers, _Opts) ->
    ok.


closest_referral({Domain, Type, Class}=Question, Namespace) ->
    closest_referral(Domain, Type, Class, Question, Namespace).

closest_referral(Domain, Type, Class, OrigQuestion, Namespace) ->
    case kurremkarmerruk_cache:lookup_resource({Domain, ns, Class}, [{namespace, Namespace}]) of
        [] when Domain =:= [] -> closest_referral_root_hints(Domain, Type, Class, OrigQuestion, Namespace);
        [_|_] = Match when ?RESOURCE_TYPE(hd(Match)) =:= ns ->
            case get_addresses(Match, Class, Namespace, []) of
                {[], _} ->
                    case Domain of
                        [] -> closest_referral_root_hints(Domain, Type, Class, OrigQuestion, Namespace);
                        _ -> closest_referral(tl(Domain), Type, Class, OrigQuestion, Namespace)
                    end;
                {Normal, Addressless} -> {ok, {OrigQuestion, referral, lists:append(Normal, [{GenTuple, []} || GenTuple <- Addressless])}}
            end;
        {name_error, _}=Tuple -> Tuple;
        _ -> closest_referral(tl(Domain), Type, Class, OrigQuestion, Namespace)
    end.

closest_referral_root_hints(_Domain, _Type, _Class, OrigQuestion, _Namespace) ->
    case
        case
            case application:get_env(kurremkarmerruk, root_server_hints) of
                undefined -> skip;
                {ok, {priv_file, App, File}}          -> {path, filename:join(code:priv_dir(App), File), []};
                {ok, {priv_file, App, File, TmpOpts}} -> {path, filename:join(code:priv_dir(App), File), TmpOpts};
                {ok, {file, TmpPath}}                 -> {path, TmpPath, []};
                {ok, {file, TmpPath, TmpOpts}}        -> {path, TmpPath, TmpOpts};
                {ok, TermList} when is_list(TermList) -> {terms, TermList}
            end
        of
            {path, Path, Opts} ->
                case dnsfile:consult(Path, Opts) of
                    {error, Reason} ->
                        error_logger:error_msg("~s~n", [dnserror_format:consult_error(Reason)]),
                        {error, no_clue};
                    {ok, _}=Tuple -> Tuple
                end;
            {terms, List} ->
                Fn = fun
                    (FunStr) when is_list(FunStr) -> dnslib:resource(FunStr);
                    (FunTerm) -> FunTerm
                end,
                {ok, lists:map(Fn, List)};
            skip -> {error, no_clue}
        end
    of
        {error, _}=ErrTuple -> ErrTuple;
        {ok, Terms} ->
            {NsRecords, AddressRecords} = lists:partition(fun (FunRR) -> ?RESOURCE_TYPE(FunRR) =:= ns end, Terms),
            FoldState = {
                maps:from_list([{dnslib:normalize_domain(?RESOURCE_DATA(GenRR)), dnslib:normalize_domain(?RESOURCE_DOMAIN(GenRR))} || GenRR <- NsRecords]),
                maps:from_list([{dnslib:normalize_domain(?RESOURCE_DOMAIN(GenRR)), {GenRR, []}} || GenRR <- NsRecords])
            },
            {_, ReferralMap} = lists:foldl(fun closest_referral_root_hints_fold/2, FoldState, AddressRecords),
            ReferralList = [ReferralTuple || {_, ReferralTuple} <- maps:to_list(ReferralMap)],
            case [ReferralTuple || ReferralTuple <- ReferralList, element(2, ReferralTuple) =/= []] of
                [] -> {error, no_clue};
                _ -> {ok, {OrigQuestion, referral, ReferralList}}
            end
    end.


closest_referral_root_hints_fold(Resource, {RefMap, NsMap}=Tuple) ->
    Key = dnslib:normalize_domain(?RESOURCE_DOMAIN(Resource)),
    case maps:get(Key, RefMap, undefined) of
        undefined -> Tuple;
        NsDomain ->
            case maps:get(NsDomain, NsMap, undefined) of
                undefined -> Tuple;
                {NSRR, List} -> {RefMap, NsMap#{NsDomain => {NSRR, [Resource|List]}}}
            end
    end.


get_addresses([], _, _, Acc) ->
    % What if we have both normal and addressless referrals?
    {Addressless, Normal} = lists:partition(fun ({_, Addresses}) -> Addresses =:= [] end, Acc),
    {Normal, [NsRr || {NsRr, _} <- Addressless]};
get_addresses([{_, _, _, _, Domain}=NsRr|Rest], Class, Namespace, Acc) ->
    % If we get name_error, we should probably drop the whole address?
    case
        case kurremkarmerruk_cache:lookup_resource({Domain, a, Class}, [{namespace, Namespace}]) of
            {{name_error, _}, _} -> name_error;
            {{nodata, _}, _} -> {ok, []};
            CaseList4 when is_list(CaseList4) -> {ok, CaseList4}
        end
    of
        name_error -> get_addresses(Rest, Class, Namespace, Acc);
        {ok, Inet} ->
            case kurremkarmerruk_cache:lookup_resource({Domain, aaaa, Class}, [{namespace, Namespace}]) of
                {{name_error, _}, _} -> get_addresses(Rest, Class, Namespace, Acc);
                {{nodata, _}, _} -> get_addresses(Rest, Class, Namespace, [{NsRr, Inet}|Acc]);
                CaseList6 when is_list(CaseList6) -> get_addresses(Rest, Class, Namespace, [{NsRr, lists:append(Inet, CaseList6)}|Acc])
            end
    end.


% Allow strategies to selectively override some answer handling. Otherwise, just do the default
% Cap the allowed chain length somehow?
response_handle_answers([], _, _, _, _, Acc) ->
    Acc;
response_handle_answers([{{Question, _, _}, Chain}|Rest], Msg, UseAf, Tried, Opts, Acc) when length(Chain) > 50 ->
    Tuple = {close_question, {original_question(Chain, Question), {error, chain_limit_exceeded}}},
    response_handle_answers(Rest, Msg, UseAf, Tried, Opts, [Tuple|Acc]);
response_handle_answers([{{Question, addressless_referral, NsRrs}=Answer, Chain}|Rest], Msg, UseAf, Tried, Opts, Acc) ->
    case proplists:get_value(cache_glue, Opts) of
        true ->
            Namespace = proplists:get_value(namespace, Opts),
            kurremkarmerruk_cache:store_interpret_results(Namespace, Answer);
        _ -> ok
    end,
    {ok, Tuple} = nsrrs_to_next_query(NsRrs, UseAf, Opts, Question, Answer, Chain),
    response_handle_answers(Rest, Msg, UseAf, Tried, Opts, [Tuple|Acc]);
response_handle_answers([{{Question, referral, _}=Answer, []}|Rest], Msg, UseAf, Tried, Opts, Acc) ->
    case proplists:get_value(cache_glue, Opts) of
        true ->
            Namespace = proplists:get_value(namespace, Opts),
            kurremkarmerruk_cache:store_interpret_results(Namespace, Answer);
        _ -> ok
    end,
    {ok, Address, Timeout} = referral_choose_address(Answer, UseAf),
    Tuple = route_question(#{}, Question, {basic, {Address, 53}}, Timeout, [Answer]),
    response_handle_answers(Rest, Msg, UseAf, Tried, Opts, [Tuple|Acc]);
response_handle_answers([{{_, referral, _}=Answer, [ChainPrev|Chain]}|Rest], Msg, UseAf, Tried, Opts, Acc) ->
    OldGlue = case ChainPrev of
        {_, referral, _} -> ChainPrev;
        {Referral, referral_with_reputation_info, _} -> Referral
    end,
    case dnsmsg:sanitize_glue(Answer, OldGlue) of
        {Question, addressless_referral, SanitizedServers} = Sanitized ->
            [{NsDomain, _, _, _, _}|_] = SanitizedServers,
            case referral_loop(dnslib:normalize_question(Question), dnslib:normalize_domain(NsDomain), [OldGlue|Chain]) of
                true ->
                    Tuple = unwind_chain({Question, server_error}, Tried, UseAf, Opts, [ChainPrev|Chain]),
                    response_handle_answers(Rest, Msg, UseAf, Tried, Opts, [Tuple|Acc]);
                false -> response_handle_answers([{Sanitized, [ChainPrev|Chain]}|Rest], Msg, UseAf, Tried, Opts, Acc)
            end;
        {Question, referral, SanitizedServers} = Sanitized ->
            % Sane referral?
            [{{NsDomain, _, _, _, _}, _}|_] = SanitizedServers,
            case referral_loop(dnslib:normalize_question(Question), dnslib:normalize_domain(NsDomain), [OldGlue|Chain]) of
                true ->
                    Tuple = unwind_chain({Question, server_error}, Tried, UseAf, Opts, [ChainPrev|Chain]),
                    response_handle_answers(Rest, Msg, UseAf, Tried, Opts, [Tuple|Acc]);
                false ->
                    % Need to check if the referral is for something we have already tried, ie. we're stuck in a loop of some kind
                    case proplists:get_value(cache_glue, Opts) of
                        true ->
                            Namespace = proplists:get_value(namespace, Opts),
                            kurremkarmerruk_cache:store_interpret_results(Namespace, Sanitized);
                        _ -> ok
                    end,
                    {ok, Address, Timeout} = referral_choose_address(Sanitized, UseAf),
                    Tuple = route_question(#{}, Question, {basic, {Address, 53}}, Timeout, [Sanitized|Chain]),
                    response_handle_answers(Rest, Msg, UseAf, Tried, Opts, [Tuple|Acc])
            end
    end;
response_handle_answers([{{Question, missing_glue_referral, _}, Chain}|Rest], Msg, UseAf, Tried, Opts, Acc) ->
    Tuple = {close_question, {original_question(Chain, Question), {error, invalid_glue}}},
    response_handle_answers(Rest, Msg, UseAf, Tried, Opts, [Tuple|Acc]);
response_handle_answers([{{{_, Type, Class}=Question, cname, {CnameRR, Answers}}=Answer, Chain}|Rest], Msg, UseAf, Tried, Opts, Acc) ->
    Tuple = case cname_loop(Answer, Chain) of
        true -> {close_question, produce_cname_loop_result(Chain, Question, Answers)};
        false ->
            {_, _, _, _, Domain} = CnameRR,
            Namespace = proplists:get_value(namespace, Opts),
            {ok, Referral} = closest_referral({Domain, Type, Class}, Namespace),
            case referral_choose_address(Referral, UseAf) of
                {ok, Address, Timeout} -> route_question(#{}, {Domain, Type, Class}, {basic, {Address, 53}}, Timeout, [Referral, Answer|Chain]);
                {error, no_suitable_address} -> {close_question, {Question, {error, no_suitable_address}}};
                {addressless_nsrrs, NsRrs} ->
                    {ok, CaseTuple} = nsrrs_to_next_query(NsRrs, UseAf, Opts, Question, Answer, Chain),
                    CaseTuple
            end
    end,
    response_handle_answers(Rest, Msg, UseAf, Tried, Opts, [Tuple|Acc]);
response_handle_answers([{{_, cname_referral, _}=Answer, [OldGlue|_]}|Rest], Msg, UseAf, Tried, Opts, Acc) ->
    [ReferralAnswer, CnameAnswer] = dnsmsg:split_cname_referral(Answer),
    response_handle_answers([{ReferralAnswer, [OldGlue, CnameAnswer|Rest]}|Rest], Msg, UseAf, Tried, Opts, Acc);
response_handle_answers([{{_, AnswerState, _}=Answer, Chain}|Rest], Msg, UseAf, Tried, Opts, Acc)
when AnswerState =:= ok; AnswerState =:= nodata; AnswerState =:= name_error ->
    Tuple = unwind_chain(Answer, Tried, UseAf, Opts, Chain),
    response_handle_answers(Rest, Msg, UseAf, Tried, Opts, [Tuple|Acc]);
response_handle_answers([{{_, Reason}=Answer, Chain}|Rest], Msg, UseAf, Tried, Opts, Acc)
when Reason =:= undefined; Reason =:= refused; Reason =:= server_error; Reason =:= format_error ->
    Tuple = unwind_chain(Answer, Tried, UseAf, Opts, Chain),
    response_handle_answers(Rest, Msg, UseAf, Tried, Opts, [Tuple|Acc]).


unwind_chain(Answer, _, _, _, []) ->
    {close_question, Answer};
unwind_chain({Question, AnswerState, {SoaRR, ErrorAnswers}}=Answer, #tried{queries=TriedQueries}=Tried, UseAf, Opts, [ChainPrev|Rest])
when AnswerState =:= name_error; AnswerState =:= nodata ->
    case ChainPrev of
        {PrevQuestion, cname, {_, PrevAnswers}} ->
            unwind_chain({PrevQuestion, AnswerState, {SoaRR, lists:append(ErrorAnswers, PrevAnswers)}}, Tried, UseAf, Opts, Rest);
        {PrevQuestion, addressless_referral, NsRrs} when AnswerState =:= name_error ->
            case proplists:get_value(cache_glue, Opts) of
                true ->
                    Namespace = proplists:get_value(namespace, Opts),
                    kurremkarmerruk_cache:store_interpret_results(Namespace, Answer);
                false -> ok
            end,
            Domain0 = element(1, Question),
            Domain = dnslib:normalize_domain(Domain0),
            case [GenTuple || {_, _, _, _, GenDomain}=GenTuple <- NsRrs, Domain =/= dnslib:normalize_domain(GenDomain)] of
                [] -> % If servers are exhausted, unwind the chain to report the authority chain error
                    [{NsDomain, _, _, _, _}|_] = NsRrs,
                    {close_question, {original_question(Rest, PrevQuestion), {error, {authority_error, authority_server_name_error, NsDomain}}}};
                Pruned ->
                    {ok, Tuple} = nsrrs_to_next_query(Pruned, UseAf, Opts, PrevQuestion, {PrevQuestion, addressless_referral, Pruned}, Rest, Tried),
                    Tuple
            end;
        {PrevQuestion, addressless_referral, NsRrs} when AnswerState =:= nodata ->
            case proplists:get_value(cache_glue, Opts) of
                true ->
                    Namespace = proplists:get_value(namespace, Opts),
                    kurremkarmerruk_cache:store_interpret_results(Namespace, Answer);
                false -> ok
            end,
            {Domain0, _, Class} = Question,
            Domain = dnslib:normalize_domain(Domain0),
            case
                case another_address_family_available(Question, UseAf) of
                    false -> new_server;
                    Type -> % Try other available address family if we haven't tried that already
                        case Tried of
                            #tried{servers=[{LatestServer, _}|_]} ->
                                case [GenTuple || {GenDomain, GenType, _}=GenTuple <- TriedQueries, GenType =:= Type, Domain =:= dnslib:normalize_domain(GenDomain)] of
                                    [] -> {ok, route_question(#{}, {Domain0, Type, Class}, LatestServer, kurremkarmerruk_recurse_address_reputation:address_wait_timeout(element(1, element(2, LatestServer))), [ChainPrev|Rest], Tried)}; % Try to get another address
                                    _ -> new_server % Try another server, if available
                                end;
                            #tried{servers=[]} -> new_server
                        end
                end
            of
                {ok, Return} -> Return;
                new_server ->
                    case [GenTuple || {_, _, _, _, GenDomain}=GenTuple <- NsRrs, Domain =/= dnslib:normalize_domain(GenDomain)] of
                        [] ->
                            [{NsDomain, _, _, _, _}|_] = NsRrs,
                            {close_question, {original_question(Rest, PrevQuestion), {error, {authority_error, no_ns_server_address, NsDomain}}}};
                        Pruned -> % Choose server to connect
                            case nsrrs_to_next_query(Pruned, UseAf, Opts, PrevQuestion, {PrevQuestion, addressless_referral, Pruned}, Rest, Tried) of
                                {ok, Tuple} -> Tuple
                            end
                    end
            end;
        {_, referral, _} -> unwind_chain({Question, AnswerState, {SoaRR, ErrorAnswers}}, Tried, UseAf, Opts, Rest);
        {_, referral_with_reputation_info, _} -> unwind_chain({Question, AnswerState, {SoaRR, ErrorAnswers}}, Tried, UseAf, Opts, Rest)
    end;
unwind_chain({_, ok, AnswerRRs}=Answer, Tried, UseAf, Opts, [ChainPrev|Rest]) ->
    Namespace = proplists:get_value(namespace, Opts),
    case ChainPrev of
        {PrevQuestion, cname, {_, PrevAnswers}} -> unwind_chain({PrevQuestion, ok, lists:append(AnswerRRs, PrevAnswers)}, Tried, UseAf, Opts, Rest);
        {_, addressless_referral, _} when AnswerRRs =:= [] -> error(no_addresses);
        {PrevQuestion, addressless_referral, _} ->
            case proplists:get_value(cache_glue, Opts) of
                true -> kurremkarmerruk_cache:store_interpret_results(Namespace, Answer);
                false -> ok
            end,
            Referral = combine_answer_addressless(AnswerRRs, ChainPrev),
            case referral_choose_address(Referral, UseAf, Tried) of
                {ok, Address, Timeout} -> route_question(#{}, PrevQuestion, {basic, {Address, 53}}, Timeout, [Referral|Rest]);
                {error, no_suitable_address} -> {close_question, {original_question([ChainPrev|Rest], PrevQuestion), {error, no_suitable_address}}};
                {addressless_nsrrs, [NsRr|_] = _NsRrs} ->
                    NsQuestion = dnslib:question(?RESOURCE_DATA(NsRr), a),
                    {ok, NsReferral} = closest_referral(NsQuestion, Namespace),
                    {ok, NsAddress, Timeout} = referral_choose_address(Referral, UseAf),
                    route_question(#{}, NsQuestion, {basic, {NsAddress, 53}}, Timeout, [NsReferral, ChainPrev|Rest])
            end;
        {_, referral, _} -> unwind_chain(Answer, Tried, UseAf, Opts, Rest);
        {_, referral_with_reputation_info, _} -> unwind_chain(Answer, Tried, UseAf, Opts, Rest);
        {_, _} -> unwind_chain(Answer, Tried, UseAf, Opts, Rest) % Keep on unwinding past previous errors
    end;
unwind_chain({Question, Error}=Answer, #tried{servers=[{ServerSpec, _}|Tail]=TriedServers}=Tried, UseAf, Opts, [ChainPrev|Chain]) ->
    case Error of
        server_error -> % Should try another server
            case referral_choose_address(ChainPrev, UseAf, Tried) of
                {ok, Address, Timeout} -> route_question(#{}, Question, {basic, {Address, 53}}, Timeout, [ChainPrev|Chain], Tried#tried{servers=[{ServerSpec, server_error}|Tail]});
                {error, no_suitable_address} -> {close_question, {original_question([ChainPrev|Chain], Question), {error, server_error}}};
                {addressless_nsrrs, NsRrs} ->
                    {ok, CaseTuple} = nsrrs_to_next_query(NsRrs, UseAf, Opts, Question, {Question, addressless_referral, NsRrs}, [Answer|Chain]),
                    CaseTuple
            end;
        format_error -> % We don't purposefully send faulty messages. If we don't have other format_errors, try another server
            case [GenTuple || {_, GenError}=GenTuple <- TriedServers, GenError =:= format_error] of
                [] ->
                    case referral_choose_address(ChainPrev, UseAf, Tried) of
                        {ok, Address, Timeout} -> route_question(#{}, Question, {basic, {Address, 53}}, Timeout, [ChainPrev|Chain], Tried#tried{servers=[{ServerSpec, format_error}|Tail]});
                        {error, no_suitable_address} ->
                            % Figure out what there was previously in the chain. Based on that, return an error
                            {close_question, {original_question([ChainPrev|Chain], Question), {error, format_error}}};
                        {addressless_nsrrs, NsRrs} ->
                            {ok, CaseTuple} = nsrrs_to_next_query(NsRrs, UseAf, Opts, Question, {Question, addressless_referral, NsRrs}, [Answer|Chain]),
                            CaseTuple
                    end;
                _ -> {close_question, {original_question([ChainPrev|Chain], Question), {error, format_error}}}
            end;
        refused -> % Might be a temporary misconfiguration. Try another server
            case length([GenTuple || {_, GenError}=GenTuple <- TriedServers, GenError =:= refused]) of
                Count when (Count + 1) >= 3 -> {close_question, {original_question([ChainPrev|Chain], Question), {error, refused}}};
                _ ->
                    case referral_choose_address(ChainPrev, UseAf, Tried) of
                        {ok, Address, Timeout} -> route_question(#{}, Question, {basic, {Address, 53}}, Timeout, [ChainPrev|Chain], Tried#tried{servers=[{ServerSpec, refused}|Tail]});
                        {error, no_suitable_address} ->
                            % Figure out what there was previously in the chain. Based on that, return an error
                            {close_question, {original_question([ChainPrev|Chain], Question), {error, refused}}};
                        {addressless_nsrrs, NsRrs} ->
                            {ok, CaseTuple} = nsrrs_to_next_query(NsRrs, UseAf, Opts, Question, {Question, addressless_referral, NsRrs}, [Answer|Chain]),
                            CaseTuple
                    end
            end;
        undefined -> % Don't know nothing. Might as well give up
            {close_question, {original_question([ChainPrev|Chain], Question), {error, undefined}}}
    end.


combine_answer_addressless([{AddressDomain0, _, _, _, _}|_] = AddressRrs, {Question, addressless_referral, NsRrs}) ->
    AddressDomain = dnslib:normalize_domain(AddressDomain0),
    case lists:splitwith(fun ({_, _, _, _, FunDomain}) -> AddressDomain =/= dnslib:normalize_domain(FunDomain) end, NsRrs) of
        {_, []} -> error(no_matching_ns);
        {Prev, [Match|Rest]} ->
            {Question, referral, [{Match, AddressRrs}|[{GenNsRr, []} || GenNsRr <- lists:append(Prev, Rest)]]}
    end.


addressless_ns_query({_, _, _, _, Domain}, Class, [inet|_]) ->
    {Domain, a, Class};
addressless_ns_query({_, _, _, _, Domain}, Class, [inet6|_]) ->
    {Domain, aaaa, Class};
addressless_ns_query(_, _, []) ->
    none.


nsrrs_to_next_query(NsRrs, UseAf, Opts, PrevQuestion, LastAnswer, Chain) ->
    nsrrs_to_next_query(NsRrs, UseAf, Opts, PrevQuestion, LastAnswer, Chain, #tried{}).

nsrrs_to_next_query([{_, _, Class, _, _}=NsRr|_] = NsRrs, UseAf, Opts, PrevQuestion, LastAnswer, Chain, Tried) ->
    Namespace = proplists:get_value(namespace, Opts),
    case get_addresses(NsRrs, Class, Namespace, []) of
        {[], _} -> % No addresses. Should we try to get addresses for a couple servers?
            case addressless_ns_query(NsRr, Class, UseAf) of
                none -> {close_question, {original_question([LastAnswer|Chain], PrevQuestion), {error, no_addresses_to_query_for}}};
                NewQuery ->
                    Namespace = proplists:get_value(namespace, Opts),
                    {ok, Referral} = closest_referral(NewQuery, Namespace),
                    case referral_choose_address(Referral, UseAf) of
                        {ok, Address, Timeout} -> {ok, route_question(#{}, NewQuery, {basic, {Address, 53}}, Timeout, [Referral, LastAnswer|Chain], Tried)};
                        {error, no_suitable_address} -> {close_question, {original_question([LastAnswer|Chain], PrevQuestion), {error, no_reroute_address}}};
                        {addressless_nsrrs, NsRrs} ->
                            {ok, CaseTuple} = nsrrs_to_next_query(NsRrs, UseAf, Opts, PrevQuestion, {PrevQuestion, addressless_referral, NsRrs}, Chain),
                            CaseTuple
                    end
            end;
        {NsAddressRrs, Addressless} ->
            Referral = {PrevQuestion, referral, lists:append(NsAddressRrs, [{AddresslessNs, []} || AddresslessNs <- Addressless])},
            case referral_choose_address(Referral, UseAf) of
                {ok, Address, Timeout} -> {ok, route_question(#{}, PrevQuestion, {basic, {Address, 53}}, Timeout, [Referral|Chain], Tried)};
                {error, no_suitable_address} -> {close_question, {original_question([LastAnswer|Chain], PrevQuestion), {error, no_reroute_address}}};
                {addressless_nsrrs, NsRrs} ->
                    {ok, CaseTuple} = nsrrs_to_next_query(NsRrs, UseAf, Opts, PrevQuestion, {PrevQuestion, addressless_referral, NsRrs}, Chain),
                    CaseTuple
            end
    end.


original_question([], Question) ->
    Question;
original_question([{Question, addressless_referral, _}|Rest], _) ->
    original_question(Rest, Question);
original_question([{Question, cname, _}|Rest], _) ->
    original_question(Rest, Question);
original_question([_|Rest], Question) ->
    original_question(Rest, Question).


another_address_family_available({_, a, _}, Afs) ->
    case lists:member(inet6, Afs) of
        true -> aaaa;
        false -> false
    end;
another_address_family_available({_, aaaa, _}, Afs) ->
    case lists:member(inet, Afs) of
        true -> a;
        false -> false
    end.


cname_loop({_, cname, {{_, cname, _, _, CnameDomain0}, _}}, Chain) ->
    Cnames = [Tuple || {_, cname, _}=Tuple <- Chain],
    CnameDomain = dnslib:normalize_domain(CnameDomain0),
    [] =/= [Tuple || {_, cname, {{Domain, cname, _, _, _}, _}}=Tuple <- Cnames, CnameDomain =:= dnslib:normalize_domain(Domain)].


produce_cname_loop_result([], Question, Resources) ->
    {Question, cname_loop, Resources};
produce_cname_loop_result([_|Rest], Question, Resources) ->
    produce_cname_loop_result(Rest, Question, Resources).


referral_loop(_, _, []) ->
    false;
referral_loop(Question, NsDomain, [{PrevQuestion, referral, [{{OldNsDomain, _, _, _, _}, _}|_]}|Rest])
when ?QUESTION_CLASS(Question) =:= ?QUESTION_CLASS(PrevQuestion) ->
    case NsDomain =:= dnslib:normalize_domain(OldNsDomain) of
        false -> referral_loop(Question, NsDomain, Rest);
        true ->
            case dnslib:normalize_question(PrevQuestion) of
                Question -> true;
                _ -> referral_loop(Question, NsDomain, Rest)
            end
    end;
referral_loop(Question, NsDomain, [{PrevQuestion, addressless_referral, [{OldNsDomain, _, _, _, _}|_]}|Rest])
when ?QUESTION_CLASS(Question) =:= ?QUESTION_CLASS(PrevQuestion) ->
    case NsDomain =:= dnslib:normalize_domain(OldNsDomain) of
        false -> referral_loop(Question, NsDomain, Rest);
        true ->
            case dnslib:normalize_question(PrevQuestion) of
                Question -> true;
                _ -> referral_loop(Question, NsDomain, Rest)
            end
    end;
referral_loop(Question, NsDomain, [_|Rest]) ->
    referral_loop(Question, NsDomain, Rest).
