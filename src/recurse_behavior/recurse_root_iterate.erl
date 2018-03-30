% ------------------------------------------------------------------------------
%
% Copyright (c) 2018, Lauri Moisio <l@arv.io>
%
% The MIT License
%
% Permission is hereby granted, free of charge, to any person obtaining a copy
% of this software and associated documentation files (the "Software"), to deal
% in the Software without restriction, including without limitation the rights
% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
% copies of the Software, and to permit persons to whom the Software is
% furnished to do so, subject to the following conditions:
%
% The above copyright notice and this permission notice shall be included in
% all copies or substantial portions of the Software.
%
% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
% THE SOFTWARE.
%
% ------------------------------------------------------------------------------
%
-module(recurse_root_iterate).

-behavior(recurse_behavior).
-export([init/4,response/5,reroute/5,stop/2]).

-include_lib("kurremkarmerruk/include/recurse.hrl").

-define(CACHE, default).


init(Questions, true, UseAf, Opts) ->
    {ok, init_questions(Questions, UseAf, Opts, []), Opts}.


init_questions([], _, _, Acc) ->
    Acc;
init_questions([OrigQuestion|Rest], UseAf, Opts, Acc) ->
    [{SendQuestion, _, _}=Referral|_] = Chain = init_referral(OrigQuestion, UseAf, []),
    ActionType = case OrigQuestion of
        {_, referral, _} -> route_question_skip_precheck;
        {_, addressless_referral, _} -> route_question_skip_precheck;
        _ -> route_question
    end,
    case kurremkarmerruk_recurse:referral_choose_address(Referral, UseAf) of
        {ok, Address} -> init_questions(Rest, UseAf, Opts, [{ActionType, {#{}, SendQuestion}, {basic, {Address, 53}}, Chain}|Acc])
    end.


init_referral(Entry, UseAf, Acc) ->
    case Entry of
        {Question, _, _}=Referral when is_tuple(Question) -> ok;
        Question -> {ok, Referral} = closest_referral(Question)
    end,
    case Referral of
        {_, referral, _} -> [Referral|Acc];
        {_, addressless_referral, [NsRr|_]} ->
            {_, _, _, _, Domain} = NsRr,
            Type = case UseAf of
                [inet|_] -> a;
                [inet6|_] -> aaaa
            end,
            init_referral({Domain, Type, in}, UseAf, [Referral|Acc])
    end.


% Should we provide a reason for reroute? What about error handling?
reroute(Questions, true, Tried, UseAf, Opts) ->
    {ok, reroute_questions(Questions, UseAf, Tried, Opts, []), Opts}.


reroute_questions([], _, _, _, Acc) ->
    Acc;
reroute_questions([{Question, [Referral|Chain]}|Rest], UseAf, Tried, Opts, Acc) ->
    Tuple = case kurremkarmerruk_recurse:referral_choose_address(Referral, UseAf, Tried) of
        {ok, Address} -> {route_question, {#{}, Question}, {basic, {Address, 53}}, [Referral|Chain], Tried};
        {error, no_suitable_address} -> {close_question, {Question, {error, no_reroute_address}}}
    end,
    reroute_questions(Rest, UseAf, Tried, Opts, [Tuple|Acc]).


response(Answers, Res, UseAf, Tried, Opts) ->
    {ok, response_handle_answers(Answers, Res, UseAf, Tried, Opts, []), Opts}.


stop(_Answers, _Opts) ->
    ok.


closest_referral({Domain, Type, Class}=Question) ->
    closest_referral(Domain, Type, Class, Question).

closest_referral(Domain, Type, Class, OrigQuestion) ->
    case kurremkarmerruk_cache:lookup_resource(?CACHE, {Domain, ns, Class}) of
        [] when Domain =:= [] -> {error, no_clue};
        [] ->
            [_|Next] = Domain,
            closest_referral(Next, Type, Class, OrigQuestion);
        Match when is_tuple(Match) ->
            [_|Next] = Domain,
            closest_referral(Next, Type, Class, OrigQuestion);
        Match when is_list(Match) ->
            case get_addresses(Match, Class, []) of
                {[], _} ->
                    case Domain of
                        [] -> {error, zones_exhausted};
                        [_|Next] -> closest_referral(Next, Type, Class, OrigQuestion)
                    end;
                %{[], Addressless} -> {ok, {OrigQuestion, addressless_referral, Addressless}};
                {Normal, Addressless} -> {ok, {OrigQuestion, referral, lists:append(Normal, [{GenTuple, []} || GenTuple <- Addressless])}}
                % What to do with a mix of addressless and normal?
            end
    end.


get_addresses([], _, Acc) ->
    % What if we have both normal and addressless referrals?
    {Addressless, Normal} = lists:partition(fun ({_, Addresses}) -> Addresses =:= [] end, Acc),
    {Normal, [NsRr || {NsRr, _} <- Addressless]};
get_addresses([{_, _, _, _, Domain}=NsRr|Rest], Class, Acc) ->
    % If we get name_error, we should probably drop the whole address?
    case
        case kurremkarmerruk_cache:lookup_resource(?CACHE, {Domain, a, Class}) of
            {{name_error, _}, _} -> name_error;
            {{nodata, _}, _} -> {ok, []};
            CaseList4 when is_list(CaseList4) -> {ok, CaseList4}
        end
    of
        name_error -> get_addresses(Rest, Class, Acc);
        {ok, Inet} ->
            case kurremkarmerruk_cache:lookup_resource(?CACHE, {Domain, aaaa, Class}) of
                {{name_error, _}, _} -> get_addresses(Rest, Class, Acc);
                {{nodata, _}, _} -> get_addresses(Rest, Class, [{NsRr, Inet}|Acc]);
                CaseList6 when is_list(CaseList6) -> get_addresses(Rest, Class, [{NsRr, lists:append(Inet, CaseList6)}|Acc])
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
        true -> kurremkarmerruk_cache:store_interpret_results(?CACHE, Answer);
        _ -> ok
    end,
    {ok, Tuple} = nsrrs_to_next_query(NsRrs, UseAf, Question, Answer, Chain),
    response_handle_answers(Rest, Msg, UseAf, Tried, Opts, [Tuple|Acc]);
response_handle_answers([{{Question, referral, _}=Answer, []}|Rest], Msg, UseAf, Tried, Opts, Acc) ->
    case proplists:get_value(cache_glue, Opts) of
        true -> kurremkarmerruk_cache:store_interpret_results(?CACHE, Answer);
        _ -> ok
    end,
    {ok, Address} = kurremkarmerruk_recurse:referral_choose_address(Answer, UseAf),
    Tuple = {route_question, {#{}, Question}, {basic, {Address, 53}}, [Answer]},
    response_handle_answers(Rest, Msg, UseAf, Tried, Opts, [Tuple|Acc]);
response_handle_answers([{{_, referral, _}=Answer, [ChainPrev|Chain]}|Rest], Msg, UseAf, Tried, Opts, Acc) ->
    OldGlue = case ChainPrev of
        {_, referral, _} -> ChainPrev;
        {Referral, referral_with_reputation_info, _} -> Referral
    end,
    case dnsmsg:sanitize_glue(Answer, OldGlue) of
        {_, addressless_referral, _} = Sanitized -> response_handle_answers([{Sanitized, [ChainPrev|Chain]}|Rest], Msg, UseAf, Tried, Opts, Acc);
        {Question, referral, _} = Sanitized ->
            case proplists:get_value(cache_glue, Opts) of
                true -> kurremkarmerruk_cache:store_interpret_results(?CACHE, Sanitized);
                _ -> ok
            end,
            {ok, Address} = kurremkarmerruk_recurse:referral_choose_address(Sanitized, UseAf),
            Tuple = {route_question, {#{}, Question}, {basic, {Address, 53}}, [Sanitized|Chain]},
            response_handle_answers(Rest, Msg, UseAf, Tried, Opts, [Tuple|Acc])
    end;
response_handle_answers([{{Question, missing_glue_referral, _}, Chain}|Rest], Msg, UseAf, Tried, Opts, Acc) ->
    Tuple = {close_question, {original_question(Chain, Question), {error, invalid_glue}}},
    response_handle_answers(Rest, Msg, UseAf, Tried, Opts, [Tuple|Acc]);
response_handle_answers([{{{_, Type, Class}=Question, cname, {CnameRR, Answers}}=Answer, Chain}|Rest], Msg, UseAf, Tried, Opts, Acc) ->
    Tuple = case cname_loop(Answer, Chain) of
        true -> {close_question, produce_cname_loop_result(Chain, Question, Answers)};
        false ->
            {_, _, _, _, Domain} = CnameRR,
            {ok, Referral} = closest_referral({Domain, Type, Class}),
            {ok, Address} = kurremkarmerruk_recurse:referral_choose_address(Referral, UseAf),
            {route_question, {#{}, {Domain, Type, Class}}, {basic, {Address, 53}}, [Referral, Answer|Chain]}
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
unwind_chain({Question, AnswerState, {SoaRR, ErrorAnswers}}=Answer, #tried{queries=TriedQueries}=Tried, UseAf, Opts, [ChainPrev|Rest]) when AnswerState =:= name_error; AnswerState =:= nodata ->
    case ChainPrev of
        {PrevQuestion, cname, {_, PrevAnswers}} ->
            unwind_chain({PrevQuestion, AnswerState, {SoaRR, lists:append(ErrorAnswers, PrevAnswers)}}, Tried, UseAf, Opts, Rest);
        {PrevQuestion, addressless_referral, NsRrs} when AnswerState =:= name_error ->
            case proplists:get_value(cache_glue, Opts) of
                true -> kurremkarmerruk_cache:store_interpret_results(?CACHE, Answer);
                false -> ok
            end,
            Domain0 = element(1, Question),
            Domain = dnslib:normalize(Domain0),
            case [GenTuple || {_, _, _, _, GenDomain}=GenTuple <- NsRrs, Domain =:= dnslib:normalize(GenDomain)] of
                [] -> % If servers are exhausted, unwind the chain to report the authority chain error
                    [{NsDomain, _, _, _, _}|_] = NsRrs,
                    {close_question, {original_question(Rest, PrevQuestion), {error, {authority_error, authority_server_name_error, NsDomain}}}};
                Pruned ->
                    {ok, Tuple} = nsrrs_to_next_query(Pruned, UseAf, PrevQuestion, {PrevQuestion, addressless_referral, Pruned}, Rest, Tried),
                    Tuple
            end;
        {PrevQuestion, addressless_referral, NsRrs} when AnswerState =:= nodata ->
            case proplists:get_value(cache_glue, Opts) of
                true -> kurremkarmerruk_cache:store_interpret_results(?CACHE, Answer);
                false -> ok
            end,
            {Domain0, _, Class} = Question,
            Domain = dnslib:normalize(Domain0),
            case
                case another_address_family_available(Question, UseAf) of
                    false -> new_server; % Try another server, if available
                    Type -> % Try another address family, if it is available and we haven't tried already
                        #tried{
                            servers=[LatestServer|_]
                        } = Tried,
                        case [GenTuple || {GenDomain, GenType, _}=GenTuple <- TriedQueries, GenType =:= Type, Domain =:= dnslib:normalize(GenDomain)] of
                            [] -> {ok, {route_question, {#{}, {Domain0, Type, Class}}, LatestServer, [ChainPrev|Rest], Tried}}; % Try to get another address
                            _ -> new_server % Try another server, if available
                        end
                end
            of
                {ok, Return} -> Return;
                new_server ->
                    case [GenTuple || {_, _, _, _, GenDomain}=GenTuple <- NsRrs, Domain =:= dnslib:normalize(GenDomain)] of
                        [] ->
                            [{NsDomain, _, _, _, _}|_] = NsRrs,
                            {close_question, {original_question(Rest, PrevQuestion), {error, {authority_error, no_ns_server_address, NsDomain}}}};
                        Pruned -> % Choose server to connect
                            {ok, Tuple} = nsrrs_to_next_query(Pruned, UseAf, PrevQuestion, {PrevQuestion, addressless_referral, Pruned}, Rest, Tried),
                            Tuple
                    end
            end;
        {_, referral, _} -> unwind_chain({Question, AnswerState, {SoaRR, ErrorAnswers}}, Tried, UseAf, Opts, Rest);
        {_, referral_with_reputation_info, _} -> unwind_chain({Question, AnswerState, {SoaRR, ErrorAnswers}}, Tried, UseAf, Opts, Rest)
    end;
unwind_chain({_, ok, AnswerRRs}=Answer, Tried, UseAf, Opts, [ChainPrev|Rest]) ->
    case ChainPrev of
        {PrevQuestion, cname, {_, PrevAnswers}} -> unwind_chain({PrevQuestion, ok, lists:append(AnswerRRs, PrevAnswers)}, Tried, UseAf, Opts, Rest);
        {_, addressless_referral, _} when AnswerRRs =:= [] -> error(no_addresses);
        {PrevQuestion, addressless_referral, _} ->
            case proplists:get_value(cache_glue, Opts) of
                true -> kurremkarmerruk_cache:store_interpret_results(?CACHE, Answer);
                false -> ok
            end,
            Referral = combine_answer_addressless(AnswerRRs, ChainPrev),
            {ok, Address} = kurremkarmerruk_recurse:referral_choose_address(Referral, UseAf, Tried),
            {route_question, {#{}, PrevQuestion}, {basic, {Address, 53}}, [Referral|Rest]};
        {_, referral, _} -> unwind_chain(Answer, Tried, UseAf, Opts, Rest);
        {_, referral_with_reputation_info, _} -> unwind_chain(Answer, Tried, UseAf, Opts, Rest)
    end;
unwind_chain({Question, Error}, #tried{servers=[{ServerSpec, _}|Tail]=TriedServers}=Tried, UseAf, _Opts, [ChainPrev|Chain]) ->
    case Error of
        server_error -> % Should try another server
            case kurremkarmerruk_recurse:referral_choose_address(ChainPrev, UseAf, Tried) of
                {ok, Address} -> {route_question, {#{}, Question}, {basic, {Address, 53}}, [ChainPrev|Chain], Tried#tried{servers=[{ServerSpec, server_error}|Tail]}};
                {error, no_suitable_address} -> {close_question, {original_question([ChainPrev|Chain], Question), {error, server_error}}}
            end;
        format_error -> % We don't purposefully send faulty messages. If we don't have other format_errors, try another server
            case [GenTuple || {_, GenError}=GenTuple <- TriedServers, GenError =:= format_error] of
                [] ->
                    case kurremkarmerruk_recurse:referral_choose_address(ChainPrev, UseAf, Tried) of
                        {ok, Address} -> {route_question, {#{}, Question}, {basic, {Address, 53}}, [ChainPrev|Chain], Tried#tried{servers=[{ServerSpec, format_error}|Tail]}};
                        {error, no_suitable_address} ->
                            % Figure out what there was previously in the chain. Based on that, return an error
                            {close_question, {original_question([ChainPrev|Chain], Question), {error, format_error}}}
                    end;
                _ -> {close_question, {original_question([ChainPrev|Chain], Question), {error, format_error}}}
            end;
        refused -> % Might be a temporary misconfiguration. Try another server
            case length([GenTuple || {_, GenError}=GenTuple <- TriedServers, GenError =:= refused]) of
                Count when (Count + 1) >= 3 -> {close_question, {original_question([ChainPrev|Chain], Question), {error, refused}}};
                _ ->
                    case kurremkarmerruk_recurse:referral_choose_address(ChainPrev, UseAf, Tried) of
                        {ok, Address} -> {route_question, {#{}, Question}, {basic, {Address, 53}}, [ChainPrev|Chain], Tried#tried{servers=[{ServerSpec, refused}|Tail]}};
                        %{addressless_nsrrs, NsRr} ->
                        {error, no_suitable_address} ->
                            % Figure out what there was previously in the chain. Based on that, return an error
                            {close_question, {original_question([ChainPrev|Chain], Question), {error, refused}}}
                    end
            end;
        undefined -> % Don't know nothing. Might as well give up
            {close_question, {original_question([ChainPrev|Chain], Question), {error, undefined}}}
    end.


combine_answer_addressless([{AddressDomain0, _, _, _, _}|_] = AddressRrs, {Question, addressless_referral, NsRrs}) ->
    AddressDomain = dnslib:normalize(AddressDomain0),
    case lists:splitwith(fun ({_, _, _, _, FunDomain}) -> AddressDomain =/= dnslib:normalize(FunDomain) end, NsRrs) of
        {_, []} -> error(no_matching_ns);
        {Prev, [Match|Rest]} ->
            {Question, referral, [{Match, AddressRrs}|[{GenNsRr, []} || GenNsRr <- lists:append(Prev, Rest)]]}
    end.


addressless_ns_query({_, _, _, _, Domain}, Class, [inet|_]) ->
    {Domain, a, Class};
addressless_ns_query({_, _, _, _, Domain}, Class, [inet6|_]) ->
    {Domain, aaaa, Class}.


nsrrs_to_next_query(NsRrs, UseAf, PrevQuestion, LastAnswer, Chain) ->
    nsrrs_to_next_query(NsRrs, UseAf, PrevQuestion, LastAnswer, Chain, #tried{}).

nsrrs_to_next_query([{_, _, Class, _, _}=NsRr|_] = NsRrs, UseAf, PrevQuestion, LastAnswer, Chain, Tried) ->
    case get_addresses(NsRrs, Class, []) of
        {[], _} -> % No addresses. Should we try to get addresses for a couple servers?
            NewQuery = addressless_ns_query(NsRr, Class, UseAf),
            {ok, Referral} = closest_referral(NewQuery),
            {ok, Address} = kurremkarmerruk_recurse:referral_choose_address(Referral, UseAf),
            {ok, {route_question, {#{}, NewQuery}, {basic, {Address, 53}}, [Referral, LastAnswer|Chain], Tried}};
        {NsAddressRrs, Addressless} ->
            Referral = {PrevQuestion, referral, lists:append(NsAddressRrs, [{AddresslessNs, []} || AddresslessNs <- Addressless])},
            {ok, Address} = kurremkarmerruk_recurse:referral_choose_address(Referral, UseAf),
            {ok, {route_question, {#{}, PrevQuestion}, {basic, {Address, 53}}, [Referral|Chain], Tried}}
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
    CnameDomain = dnslib:normalize(CnameDomain0),
    [] =/= [Tuple || {_, cname, {{Domain, cname, _, _, _}, _}}=Tuple <- Cnames, CnameDomain =:= dnslib:normalize(Domain)].


produce_cname_loop_result([], Question, Resources) ->
    {Question, cname_loop, Resources};
produce_cname_loop_result([_|Rest], Question, Resources) ->
    produce_cname_loop_result(Rest, Question, Resources).
