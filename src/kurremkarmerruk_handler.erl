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
-module(kurremkarmerruk_handler).
-export([
    execute_handlers/2,
    add_answers/3,
    current_answers/2,
    get_handlers/2,
    is_preceeded_by/3
]).

-include_lib("kernel/include/logger.hrl").

-callback spawn_handler_proc()
    -> boolean()
     | {M :: atom(), F :: atom(), A :: list(term())}
     | {M :: atom(), F :: atom(), A :: list(term()), Opts :: map()}.
-callback execute(term(), term())
    -> {'ok', term()}
     | {'passthrough', [{Len :: 0..16#FFFF, iolist()}]} % Allow a form which is a list of binaries, which the
     | {'stop', term()}
     | 'drop'.
-callback valid_opcodes() -> atom() | list(atom()).
-callback config_init(map()) -> map().
-callback config_keys() -> [atom()].
-callback handle_config(Key :: atom(), Config :: term(), Namespace :: map())
    -> {'ok', map()} | {'error', Reason :: term()}.
-callback config_end(map()) -> map().

-optional_callbacks([handle_config/3]).


-spec execute_handlers(dnsmsg:message(), map() | 'undefined') -> {'ok', dnsmsg:message()} | 'drop'.
execute_handlers(Msg = #{'Opcode' := Opcode}, _) when is_integer(Opcode) ->
    {ok, dnsmsg:response(Msg, #{return_code => not_implemented})};
execute_handlers(Msg, undefined) ->
    {ok, dnsmsg:response(Msg, #{return_code => refused})};
execute_handlers(Msg0 = #{'Opcode' := Opcode}, Namespace = #{recurse := RecursionAvailable}) ->
    % Should an ID be globally unique, or just for that address/port combo?
    % This will prevent parallel handing of requests, but not for example a case
    % where the original message processing completes and after that a previously
    % buffered duplicate of that same message is undertaken again.
    %
    % Though repeatedly received message can act as a hint as to the timeout value of
    % the sending client...
    %
    % What will happen if a message is ignored by each and every handler?
    % Should add a flag that answers have been added to message... Or just pattern match it against the original?
    Hash = hash_message(Msg0),
    case ets:insert_new(client_requests, {Hash}) of
        false ->
            drop;
        true ->
            case get_handlers(Opcode, Namespace) of
                not_implemented -> dnsmsg:response(Msg0, #{recursion_available => RecursionAvailable, return_code => refused});
                Handlers ->
                    Msg1 = dnsmsg:set_response_header(Msg0, [{return_code, ok}, {authoritative, true}, {recursion_available, RecursionAvailable}]),
                    Msg2 = Msg1#{questions => dnslib:deduplicate(dnsmsg:questions(Msg0))},
                    try execute_handlers(Handlers, Msg2, Namespace#{opcode => Opcode}) of
                        drop -> drop;
                        {ok, Msg3} ->
                            Answers = finalize_answers(maps:get(handler_answers, Msg3, [])),
                            case lists:partition(fun (FunTuple) -> is_tuple(element(2, FunTuple)) end, Answers) of
                                {[], _} -> {ok, Msg4} = dnsmsg:apply_interpret_results(Answers, Msg3#{'Questions' := []});
                                {_Errors, _} ->
                                    % When we have errors (for any reason, return server_error)
                                    Msg4 = dnsmsg:set_response_header(Msg3, return_code, server_error)
                            end,
                            {ok, dnsmsg:response(Msg4)};
                        {passthrough, _}=Tuple -> Tuple;
                        {server_error, Handler, Reason, Stacktrace} ->
                            ?LOG_ERROR("Handler ~p produced an exception ~p, Stacktrace: ~p~n", [Handler, Reason, Stacktrace]),
                            {ok, dnsmsg:response(Msg0, #{return_code => server_error})}
                    after
                        ets:delete(client_requests, Hash)
                    end
            end
    end.


hash_message(Msg = #{'EDNS' := _}) ->
    #{
        'ID'                 := Id,
        'Opcode'             := Opcode,
        'Recursion_desired'  := RecursionDesired,
        'Authenticated_data' := AD,
        'Checking_disabled'  := CD,
        'Questions'          := Questions,
        %'Answers'            := Answers,
        %'Nameservers'        := Nameservers,
        %'Additional'         := Additional,
        'EDNS_version'       := Version,
        'EDNS_dnssec_ok'     := DnsSecOk
    } = Msg,
    NormalizedQuestions = lists:sort(lists:map(fun dnslib:normalize_question/1, Questions)),
    erlang:phash2({Id, Opcode, RecursionDesired, AD, CD, Version, DnsSecOk, NormalizedQuestions});
hash_message(Msg) ->
    #{
        'ID'                 := Id,
        'Opcode'             := Opcode,
        'Recursion_desired'  := RecursionDesired,
        'Authenticated_data' := AD,
        'Checking_disabled'  := CD,
        'Questions'          := Questions
        %'Answers'            := Answers,
        %'Nameservers'        := Nameservers,
        %'Additional'         := Additional,
    } = Msg,
    NormalizedQuestions = lists:sort(lists:map(fun dnslib:normalize_question/1, Questions)),
    erlang:phash2({Id, Opcode, RecursionDesired, AD, CD, NormalizedQuestions}).


-ifdef(OTP_RELEASE).
execute_handlers([], Msg, _) ->
    {ok, Msg};
execute_handlers(_, Msg = #{questions := []}, _) ->
    {ok, Msg};
execute_handlers([Handler|Rest], Msg0, Namespace) ->
    try Handler:execute(Msg0, Namespace) of
        {ok, Msg1}   -> execute_handlers(Rest, Msg1, Namespace);
        {stop, Msg1} -> {ok, Msg1};
        {passthrough, _}=Tuple -> Tuple;
        drop         -> drop
        % Should we catch and report invalid returns?
    catch
        % Allow handlers to return with throw, like gen_statem
        error:Reason:Stacktrace -> {server_error, Handler, Reason, Stacktrace};
        {ok, Msg1}   -> execute_handlers(Rest, Msg1, Namespace);
        {stop, Msg1} -> {ok, Msg1};
        {passthrough, _}=Tuple -> Tuple;
        drop         -> drop
    end.
-else.
execute_handlers([], Msg, _) ->
    {ok, Msg};
execute_handlers(_, Msg = #{questions := []}, _) ->
    {ok, Msg};
execute_handlers([Handler|Rest], Msg0, Namespace) ->
    try Handler:execute(Msg0, Namespace) of
        {ok, Msg1}   -> execute_handlers(Rest, Msg1, Namespace);
        {stop, Msg1} -> {ok, Msg1};
        {passthrough, _}=Tuple -> Tuple;
        drop         -> drop
        % Should we catch and report invalid returns?
    catch
        % Allow handlers to return with throw, like gen_statem
        error:Reason -> {server_error, Handler, Reason, erlang:get_stacktrace()};
        {ok, Msg1}   -> execute_handlers(Rest, Msg1, Namespace);
        {stop, Msg1} -> {ok, Msg1};
        {passthrough, _}=Tuple -> Tuple;
        drop         -> drop
    end.
-endif.


finalize_answers(Answers) ->
    % Find cnames, check if we have final responses for them.
    % Or start with final responses ok/nodata/name_error chase cnames to them?
    case lists:partition(fun final_answer_fun/1, Answers) of
        {Final, []} -> drop_duplicate_answers(Final, #{});
        {Final, Other} -> chase_cnames(Final, Other, [])
    end.


final_answer_fun({_, cname, _}) -> false;
final_answer_fun(_) -> true.


chase_cnames_fun(Domain) ->
    fun
        ({_, cname, {CnameRr, _}}) -> dnslib:normalize_domain(element(5, CnameRr)) =:= Domain
    end.


chase_cnames([], Others, Acc) ->
    drop_duplicate_answers(lists:append(lists:reverse(Acc), Others), #{});
chase_cnames([Answer|Rest], Others, Acc) ->
    Question = element(1, Answer),
    Domain = dnslib:normalize_domain(element(1, Question)),
    case lists:partition(chase_cnames_fun(Domain), Others) of
        {[], _} -> chase_cnames(Rest, Others, [Answer|Acc]);
        {[{OriginalQuestion, cname, {CnameRr, CnameResources}}], Others1} ->
            AnswerType = element(2, Answer),
            Answer1 = case AnswerType of
                ok -> setelement(3, Answer, lists:append(element(3, Answer), CnameResources));
                referral -> {OriginalQuestion, cname_referral, {CnameRr, Answer, CnameResources}};
                _ when AnswerType =:= nodata; AnswerType =:= name_error ->
                    {SoaRr, Resources} = element(3, Answer),
                    setelement(3, Answer, {SoaRr, lists:append(Resources, CnameResources)});
                {_, _} -> Answer
            end,
            Answer2 = setelement(1, Answer1, OriginalQuestion),
            Rest1 = [GenTuple || GenTuple <- Rest, dnslib:normalize_domain(element(1, element(1, GenTuple))) =/= Domain],
            % Since we know that this or that Domain is a followed cname, we should drop all other
            % results for that domain
            chase_cnames(Rest1, Others1, [Answer2|Acc])
    end.


% Some answers are terminal, other in progress. Final boolean in addition to Authoritative
add_answers([], _, Message) ->
    Message;
add_answers(Answers, Authoritative, Message) ->
    CurrentAuthoritative = maps:get('Authoritative', maps:get('Response', Message), true),
    Message1 = dnsmsg:set_response_header(Message, authoritative, Authoritative andalso CurrentAuthoritative),
    add_answers1(Answers, maps:get(handler_answers, Message1, []), Message1).

add_answers1([], Acc, Message) ->
    Message#{handler_answers => Acc};
add_answers1([{_, _}=Tuple|Rest], Acc, Message) ->
    add_answers1(Rest, [Tuple|Acc], Message);
add_answers1([{Question, AnswerType, _}=Tuple|Rest], Acc, Message = #{questions := Questions})
when AnswerType =:= ok; AnswerType =:= nodata; AnswerType =:= name_error; AnswerType =:= cname_loop ->
    Questions1 = [GenTuple || GenTuple <- Questions, GenTuple =/= Question],
    add_answers1(Rest, [Tuple|Acc], Message#{questions => Questions1});
add_answers1([{{_, Type, Class}=Question, cname, {{_, _, _, _, CnameDomain}, _}}=Tuple|Rest], Acc, Message = #{questions := Questions}) ->
    Questions1 = [GenTuple || GenTuple <- Questions, GenTuple =/= Question],
    add_answers1(Rest, [Tuple|Acc], Message#{questions => [{CnameDomain, Type, Class}|Questions1]});
add_answers1([{_, ReferralType, _}=Tuple|Rest], Acc, Message)
when ReferralType =:= referral; ReferralType =:= addressless_referral ->
    add_answers1(Rest, [Tuple|Acc], Message);
add_answers1([{{_, Type, Class}=Question, cname_referral, {{_, _, _, _, CnameDomain}=CnameRr, Referral, Resources}}|Rest], Acc, Message = #{questions := Questions}) ->
    Questions1 = [GenTuple || GenTuple <- Questions, GenTuple =/= Question],
    NewQuestion = {CnameDomain, Type, Class},
    CnameTuple = {Question, cname, {CnameRr, Resources}},
    add_answers1(Rest, [Referral, CnameTuple|Acc], Message#{questions => [NewQuestion|Questions1]}).


drop_duplicate_answers([], Answers) ->
    [GenAnswer || {_, GenAnswer} <- maps:to_list(Answers)];
drop_duplicate_answers([Answer|Rest], Answers) ->
    Question = dnslib:normalize_question(element(1, Answer)),
    case maps:get(Question, Answers, undefined) of
        undefined -> drop_duplicate_answers(Rest, Answers#{Question => Answer});
        _ -> drop_duplicate_answers(Rest, Answers)
    end.


get_handlers(Opcode, #{handlers := HandlersMap}) ->
    maps:get(Opcode, HandlersMap, maps:get('_', HandlersMap, not_implemented)).


is_preceeded_by(What, ByWhich, Namespace = #{opcode := Opcode}) ->
    case get_handlers(Opcode, Namespace) of
        not_implemented -> false;
        [] -> false;
        List ->
            {Before, _} = lists:splitwith(fun (Handler) -> Handler =/= What end, List),
            lists:member(ByWhich, Before)
    end.


current_answers(Question, Msg) ->
    Answers = maps:get(handler_answers, Msg, []),
    [GenTuple || GenTuple <- Answers, element(1, GenTuple) =:= Question].
