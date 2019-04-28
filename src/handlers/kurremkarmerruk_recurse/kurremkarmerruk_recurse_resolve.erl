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
-module(kurremkarmerruk_recurse_resolve).


-export([
    route_question/5,
    route_question/6,
    resolve/1,
    resolve/2,
    resolve_init/2,
    task_open_questions/1
]).

-include_lib("kernel/include/logger.hrl").
-include_lib("dnslib/include/dnslib.hrl").
-include_lib("kurremkarmerruk/include/recurse.hrl").

-import(kurremkarmerruk_recurse_address_reputation, [
    available_afs/0,
    udp_timeout/2
]).

-record(recurse_task, {
    pid,
    ref,
    previous_task,
    namespace,
    open_questions,
    answers=[],
    pending_questions=[],
    timeout,
    strategy,
    strategy_state,
    zone_available=false,
    cache_available=false,
    timeouts=[]
}).


route_question(MsgOpts, Question, ServerSpec, Timeout, State) ->
    route_question(MsgOpts, Question, ServerSpec, Timeout, State, #tried{}).

route_question(MsgOpts, Question, {_, _} = ServerSpec, Timeout, State, #tried{} = Tried)
when is_map(MsgOpts), ?IS_QUESTION(Question), is_integer(Timeout) ->
    {route_question, {MsgOpts, Question}, ServerSpec, Timeout, State, Tried}.


resolve(Query) ->
    resolve(Query, [{namespace, namespace:internal()}]).


%-spec resolve([dnslib:question()] | dnslib:question(), map()) -> list().
resolve({[Head|_]=Domain0, Type, Class}, Opts) when is_integer(Head) ->
    {ok, _, Domain1} = dnslib:list_to_domain(Domain0),
    Domain2 = case Domain1 of
        ['_'|Tail] -> [<<"*">>|Tail];
        _ -> Domain1
    end,
    resolve({Domain2, Type, Class}, Opts);
resolve(Query, Opts) when is_list(Opts), is_tuple(Query) ->
    resolve([Query], Opts);
resolve(Query, Opts0) when is_list(Opts0) ->
    % If Opts contains async, spawn a process to handle the request...
    Opts = case lists:keymember(namespace, 1, Opts0) of
        true -> Opts0;
        false -> [{namespace, namespace:internal()}|Opts0]
    end,
    case lists:member(async, Opts) of
        false -> resolve_init(Query, Opts);
        true ->
            Ref = make_ref(),
            Pid = spawn_link(?MODULE, resolve_init, [Query, [{ref, Ref}, {pid, self()}|Opts]]),
            {ok, Pid, Ref}
    end.


% How to handle message stream?
resolve_init(Query, Opts) ->
    Task0 = #recurse_task{
        pid = proplists:get_value(pid, Opts),
        ref = proplists:get_value(ref, Opts),
        open_questions=Query
    },
    {Namespace, _} = case proplists:get_value(namespace, Opts) of
        undefined -> {undefined, undefined};
        CaseSpace -> {CaseSpace, maps:get(namespace_id, CaseSpace)}
    end,
    #recurse_task{open_questions=OpenQuestions} = Task1 = case Namespace of
        undefined -> Task0;
        #{recurse_config := CaseConfig} ->
            #{
                recurse_unknown_types := AllowUnknownTypes,
                recurse_unknown_classes := AllowUnknownClasses,
                recurse_disallow_types := TypeList,
                recurse_disallow_classes := ClassList
            } = CaseConfig,
            Zone = kurremkarmerruk_handler:is_preceeded_by(kurremkarmerruk_recurse, kurremkarmerruk_zone, Namespace),
            Cache = kurremkarmerruk_handler:is_preceeded_by(kurremkarmerruk_recurse, kurremkarmerruk_cache, Namespace),
            #recurse_task{open_questions=CaseQuestions0,answers=CaseAnswers0} = Task0,
            PartFun = fun
                ({_, axfr, _}) -> false;
                ({_, ixfr, _}) -> false;
                ({_, Type, Class}) when is_integer(Type), is_integer(Class) -> AllowUnknownTypes andalso AllowUnknownClasses;
                ({_, Type, Class}) when is_integer(Type) -> AllowUnknownTypes andalso not lists:member(Class, ClassList);
                ({_, Type, Class}) when is_integer(Class) -> not lists:member(Type, TypeList) andalso AllowUnknownClasses;
                ({_, Type, Class}) -> not (lists:member(Type, TypeList) andalso lists:member(Class, ClassList))
            end,
            {Send, Refuse} = lists:partition(PartFun, CaseQuestions0),
            Task0#recurse_task{
                cache_available=Cache,
                zone_available=Zone,
                open_questions=Send,
                answers=lists:append([{GenQuestion, refused} || GenQuestion <- Refuse], CaseAnswers0)
            }
    end,
    {{RecurseMod, InitArgs0}, Config} = case Namespace of
        undefined -> {?ITERATE_STRATEGY, true}; % Should the default strategy be configurable?
        #{recurse_config := RecurseConfig} -> {maps:get(recurse_strategy, RecurseConfig), maps:get(servers, RecurseConfig)}
    end,
    InitArgs = case is_list(InitArgs0) of
        true ->
            case lists:member(include_namespace, InitArgs0) of
                true -> [{namespace, Namespace}|lists:delete(include_namespace, InitArgs0)];
                false -> InitArgs0
            end;
        false -> InitArgs0
    end,
    {ok, Actions, RecurseState} = RecurseMod:init(OpenQuestions, Config, available_afs(), InitArgs),
    resolve_handle_actions(Actions, Task1#recurse_task{strategy=RecurseMod,strategy_state=RecurseState,namespace=Namespace}, [], make_ref()).

resolve_handle_actions(Actions, Task0, Messages, Ref) ->
    case handle_recursed_actions(Actions, Task0, []) of
        {[], Task = #recurse_task{open_questions=[],previous_task=undefined}} ->
            resolve_end(Task);
        % Add case for subtasks
        {NewMessages, Task1} ->
            case send_routed(NewMessages, Ref) of
                ok -> resolve_wait_responses(Task1, lists:append(NewMessages, Messages), Ref);
                {rename, Renames} -> resolve_wait_responses(Task1, lists:append(rename_messages(Renames, NewMessages, []), Messages), Ref)
            end
    end.

rename_messages([], [], Acc) ->
    Acc;
rename_messages([{Old,New}|Rest], Messages, Acc) ->
    {[Renamed], Other} = lists:partition(fun (Msg) -> dnsmsg:id(Msg) =:= Old end, Messages),
    rename_messages(Rest, Other, [Renamed#{'ID' => New}|Acc]).


send_routed([{ServerSpec0, Message}|Rest] = All, Ref) when is_map(Message) ->
    Id = dnsmsg:id(Message),
    {ok, _, Bin} = dnswire:to_binary(Message),
    case element(1, ServerSpec0) of
        basic when byte_size(Bin) =< 512 -> % We need the info on UDP payload size here...
            {Name, Af} = case tuple_size(element(1, element(2, ServerSpec0))) of
                4 -> {kurremkarmerruk_recurse_udp_inet, inet};
                8 -> {kurremkarmerruk_recurse_udp_inet6, inet6}
            end,
            Pid = case whereis(Name) of
                undefined ->
                    {ok, CasePid} = kurremkarmerruk_recurse_server:udp_socket(Af),
                    CasePid;
                CasePid -> CasePid
            end,
            Mon = monitor(process, Pid),
            Pid ! {send, self(), Ref, element(2, ServerSpec0), Bin},
            receive
                {send_ack, _, Ref, Id} ->
                    true = demonitor(Mon),
                    send_routed(Rest, Ref); % What if the socket needs to rename the message?
                {'DOWN', Mon, process, Pid, _} -> send_routed(All, Ref)
            after
                50 ->
                    ?LOG_ERROR("No ack for message send"),
                    true = demonitor(Mon),
                    send_routed(All, Ref)
            end;
        _ ->
            {ok, Pid} = kurremkarmerruk_recurse_server:connect({ServerSpec0, []}),
            % Task the process...
            Mon = monitor(process, Pid),
            Pid ! {send, self(), Ref, element(2, ServerSpec0), Bin},
            receive
                {send_ack, _, Ref, Id} -> % What if the socket needs to rename the message?
                    true = demonitor(Mon),
                    send_routed(Rest, Ref);
                {'DOWN', Mon, process, Pid, _} -> send_routed(All, Ref)
            after
                50 ->
                    ?LOG_ERROR("No ack for message send"),
                    true = demonitor(Mon),
                    send_routed(All, Ref)
            end
    end;
send_routed([], _) ->
    ok.


resolve_wait_responses(Task, [], _) ->
    resolve_end(Task);
resolve_wait_responses(Task = #recurse_task{timeouts=Timeouts}, Messages, Ref) ->
    % Since going over the network is always going to take the
    % some number of times the time it takes us to figure out a response locally,
    % allow long-running handlers to defer until a message and free the workers
    % to tackle a couple other messages while waiting... Though that complicates timeout handling somewhat...
    % We should also consider task deadline, if there is one...
    {Deadline, TimeoutId}=TimeoutTuple = lists:foldl(
        fun
            (FunTuple, {nil, _}) -> FunTuple;
            ({TupleTimeout, _}=FunTuple, {CurTimeout, _}) when TupleTimeout < CurTimeout -> FunTuple;
            (_, FunTuple) -> FunTuple
        end, {nil, nil}, Timeouts),
    Timeout = max(0, Deadline - erlang:monotonic_time(millisecond)),
    receive
        % If we want to try saving recursed requests,
        % we might want to track sent questions and
        % refer one recursing process to another.
        % Then as the one doing the resolving is done,
        % it can send the answers it received to the other.
        % Suppose this assumes that both are in the same namespace.
        % Add a field to record for pending pids.
        % Though are "allowed" to share a resource if it has TTL=0?
        % If we take TTL=0 to mean that each and every request has to
        % be satisfied by the authoritative server, we'd just be introducing
        % latency into the other pending clients while achieving little else...
        {recurse_response, _, Ref, Address, _Port, Latency, Bin} ->
            {ok, Message, <<>>} = dnswire:from_binary(Bin),
            kurremkarmerruk_recurse_address_reputation:update_address_reputation(Address, Latency, dnsmsg:udp_payload_max_size(Message)),
            Id = dnsmsg:id(Message),
            case lists:partition(fun ({_, FunMsg}) -> dnsmsg:id(FunMsg) =:= Id end, Messages) of
                {[{_, Original}], Rest} -> resolve_handle_response(Message, Original, Task#recurse_task{timeouts=[GenTuple || {_, GenId}=GenTuple <- Timeouts, GenId =/= Id]}, Rest, Ref);
                {[], _} ->
                    % A message arrived past deadline... What should we do?
                    resolve_wait_responses(Task, Messages, Ref)
            end;
        {recurse_reroute, Ref, Id, Reason} ->
            case lists:partition(fun ({_, FunMsg}) -> dnsmsg:id(FunMsg) =:= Id end, Messages) of
                {[{_, Rerouted}], Rest} ->
                    #{tried := Tried, 'ID' := ReroutedId} = Rerouted,
                    #tried{servers=[{{_, {Address, _}}=ServerSpec, nil}|Prev],afs=TriedAfs} = Tried,
                    Tried1 = case Reason of
                        bad_af when tuple_size(Address) =:= 4 -> Tried#tried{servers=[{ServerSpec, Reason}|Prev], afs=[inet|TriedAfs]};
                        bad_af when tuple_size(Address) =:= 8 -> Tried#tried{servers=[{ServerSpec, Reason}|Prev], afs=[inet6|TriedAfs]};
                        _ -> Tried#tried{servers=[{ServerSpec, Reason}|Prev]}
                    end,
                    resolve_reroute_message(Rerouted#{tried => Tried1}, Task#recurse_task{timeouts=[GenTuple || {_, GenId}=GenTuple <- Timeouts, GenId =/= ReroutedId]}, Rest, Ref);
                {[], _} ->
                    resolve_wait_responses(Task, Messages, Ref)
            end
    after
        Timeout ->
            {[{_, Timeouted}], Rest} = lists:partition(fun ({_, FunMsg}) -> dnsmsg:id(FunMsg) =:= TimeoutId end, Messages),
            #{tried := Tried} = Timeouted,
            #tried{servers=[{{_, {Address, _}}=ServerSpec, nil}|Prev]} = Tried,
            % It doesn't necessarily make sense to timeout like this, because the message might just be
            % delayed and arrive at some point in the future...
            udp_timeout(Address, dnsmsg:udp_payload_max_size(Timeouted)),
            resolve_reroute_message(Timeouted#{tried => Tried#tried{servers=[{ServerSpec, timeout}|Prev]}}, Task#recurse_task{timeouts=lists:delete(TimeoutTuple, Timeouts)}, Rest, Ref)
    end.


chain_for_message(#{'Questions' := Question}, Task) ->
    chain_for_question(Question, Task).

chain_for_question([Question0], Task = #recurse_task{pending_questions=Pending}) ->
    Question = dnslib:normalize_question(Question0),
    {[Chain], Rest} = lists:partition(fun (FunTuple) -> Question =:= dnslib:normalize_question(element(1, FunTuple)) end, Pending),
    {element(2, Chain), Task#recurse_task{pending_questions=Rest}};
chain_for_question(Question, Task) when is_tuple(Question) ->
    chain_for_question([Question], Task).


resolve_handle_response(#{'Truncated' := true}, Original = #{tried := Tried, 'Questions' := [Question]}, Task0, Rest, Ref) ->
    #tried{servers=[{{basic, {Address, _} = AddressPort}, _}|_]} = Tried,
    ServerSpec = {tcp, AddressPort},
    Timeout = kurremkarmerruk_recurse_address_reputation:address_wait_timeout(Address),
    {Chain, Task1} = chain_for_message(Original, Task0),
    resolve_handle_actions([route_question(#{}, Question, ServerSpec, Timeout, Chain, Tried)], Task1, Rest, Ref);
resolve_handle_response(Message, Original, Task0, Rest, Ref) ->
    #recurse_task{
        pending_questions=Pending0,
        strategy=RecurseMod,
        strategy_state=RecurseState0,
        zone_available=Zone,
        namespace=Namespace
    } = Task0,
    {ok, Answers0} = dnsmsg:interpret_response(restore_sent_questions(Message, Original)),
    Answers1 = case Zone of
        true -> kurremkarmerruk_zone:override_authoritative(Answers0, Namespace, []);
        false -> Answers0
    end,
    {Matched, Pending1} = match_answers_to_pending(Answers1, Pending0, []),
    Task1 = Task0#recurse_task{pending_questions=Pending1},
    {ok, Actions, RecurseState1} = RecurseMod:response(Matched, Message, available_afs(), tried(Original), RecurseState0),
    resolve_handle_actions(Actions, Task1#recurse_task{pending_questions=Pending1,strategy_state=RecurseState1}, Rest, Ref).


resolve_reroute_message(Message, Task0, Rest, Ref) ->
    #{
        tried := Tried,
        'Questions' := Questions
    } = Message,
    #recurse_task{
        pending_questions=Pending0,
        strategy=RecurseMod,
        strategy_state=RecurseState0,
        namespace=#{recurse_config := Config}
    } = Task0,
    {Matched, Pending1} = match_questions_to_pending(Questions, Pending0, []),
    {ok, Actions, RecurseState1} = RecurseMod:reroute(Matched, maps:get(servers, Config), Tried, available_afs(), RecurseState0),
    resolve_handle_actions(Actions, Task0#recurse_task{pending_questions=Pending1,strategy_state=RecurseState1}, Rest, Ref).


resolve_end(Task) ->
    #recurse_task{
        pid=Pid,
        ref=Ref,
        strategy=RecurseMod,
        strategy_state=RecurseState,
        answers=Answers,
        namespace=Namespace
    } = Task,
    RecurseMod:stop(Answers, RecurseState),
    {MaxTtl, MaxNegTtl} = case Namespace of
        undefined -> {86400, 86400};
        #{max_ttl := CaseTtl, max_negative_ttl := CaseNegTtl} -> {CaseTtl, CaseNegTtl}
    end,
    case Pid of
        undefined -> {ok, kurremkarmerruk_utils:limit_answer_ttls(Answers, MaxTtl, MaxNegTtl, [])};
        _ -> Pid ! {dns, Ref, kurremkarmerruk_utils:limit_answer_ttls(Answers, MaxTtl, MaxNegTtl, [])}
    end.


handle_recursed_actions([], Task, Acc) ->
    {Acc, Task};
handle_recursed_actions([{close_question, Answer}|Rest], Task = #recurse_task{answers=Answers,open_questions=Questions}, Acc) ->
    Question = element(1, Answer),
    handle_recursed_actions(Rest, Task#recurse_task{answers=[Answer|Answers],open_questions=lists:delete(dnslib:normalize_question(Question), Questions)}, Acc);
handle_recursed_actions([{route_question, _Args, _, _, _, #tried{}}=Action|Rest], Task, Acc)->
    handle_recursed_route_question_precheck(Action, Rest, Task, Acc);
handle_recursed_actions([{route_question_skip_precheck, _Args, _, _, _, #tried{}}=Action|Rest], Task, Acc) ->
    handle_recursed_route_question(Action, Rest, Task, Acc).


handle_recursed_route_question_precheck({_, {_, Question}, _, _, Chain, Tried} = Action, Rest, Task = #recurse_task{namespace=Namespace,zone_available=Zone,cache_available=Cache}, Acc) ->
    case Question of
        [] -> error(no_questions);
        _ -> ok
    end,
    case [GenPrev || {true, GenPrev} <- [{Zone, kurremkarmerruk_zone}, {Cache, kurremkarmerruk_cache}]] of
        [] -> handle_recursed_route_question(Action, Rest, Task, Acc);
        Previous ->
            % execute_query for both, figure out if either produced an answer to the question.
            case got_answer(lists:foldl(fun (FunMod, FunAcc) -> lists:append(FunMod:execute_query(Question, Namespace), FunAcc) end, [], Previous), Chain) of
                false -> handle_recursed_route_question(Action, Rest, Task, Acc);
                {true, Answer} ->
                    #recurse_task{
                        strategy=RecurseMod,
                        strategy_state=RecurseState0
                    } = Task,
                    {ok, FurtherActions, RecurseState1} = RecurseMod:response([{Answer, Chain}], nil, available_afs(), Tried, RecurseState0),
                    Task1 = Task#recurse_task{strategy_state=RecurseState1},
                    {NewMessages, Task2} = handle_recursed_actions(FurtherActions, Task1, []),
                    handle_recursed_actions(Rest, Task2, lists:append(NewMessages, Acc))
            end
    end.


got_answer([], _) ->
    false;
got_answer([Answer|Rest], Chain) when tuple_size(Answer) =:= 2 ->
    % This case is more or less specifically for zone.
    % Is there a better/more clear way to do this?
    got_answer(Rest, Chain);
got_answer([{_, Type, _}=Answer|Rest], Chain)
when Type =:= referral; Type =:= addressless_referral; Type =:= missing_glue_referral ->
    case lists:member(Answer, Chain) of
        true -> got_answer(Rest, Chain);
        false ->
            % Should prolly have some nicer way to do this...
            % Basically we check if we got a better referral from cache/zone
            PrevReferral = hd([Gen || Gen <- Chain, element(2, Gen) =:= referral]),
            PrevReferralDomain = element(1, element(1, hd(element(3, PrevReferral)))),
            CurReferralDomain = element(1, element(1, hd(element(3, Answer)))),
            case dnslib:is_subdomain(dnslib:normalize_domain(CurReferralDomain), dnslib:normalize_domain(PrevReferralDomain)) of
                true -> {true, Answer};
                false -> got_answer(Rest, Chain)
            end
    end;
got_answer([Answer|_], _) when tuple_size(Answer) =/= 2 ->
    {true, Answer}.


handle_recursed_route_question({_, Args, Server, Timeout, Chain, Tried}, Rest, Task = #recurse_task{pending_questions=Pending,timeouts=Timeouts}, Acc) ->
    Question = element(2, Args),
    #tried{servers=TriedServers, queries=TriedQueries} = Tried,
    Message0 = apply(dnsmsg, new, tuple_to_list(Args)),
    Message1 = Message0#{
        tried => Tried#tried{servers=[{Server, nil}|TriedServers], queries=[Question|TriedQueries]}
    },
    handle_recursed_actions(Rest, Task#recurse_task{pending_questions=[{dnslib:normalize_question(Question), Chain}|Pending],timeouts=[{erlang:monotonic_time(millisecond) + Timeout, dnsmsg:id(Message0)}|Timeouts]}, [{Server, Message1}|Acc]).


match_answers_to_pending([], Pending, Acc) ->
    {Acc, Pending};
match_answers_to_pending([Tuple|Rest], Pending0, Acc) ->
    % It could be that we have no match, since a previously received message cleared it.
    {value, {_, Chain}, Pending1} = lists:keytake(dnslib:normalize_question(element(1, Tuple)), 1, Pending0),
    match_answers_to_pending(Rest, Pending1, [{Tuple, Chain}|Acc]).


match_questions_to_pending([], Pending, Acc) ->
    {Acc, Pending};
match_questions_to_pending([Question|Rest], Pending0, Acc) ->
    % It could be that we have no match, since a previously received message cleared it.
    {value, {_, Chain}, Pending1} = lists:keytake(dnslib:normalize_question(Question), 1, Pending0),
    match_questions_to_pending(Rest, Pending1, [{Question, Chain}|Acc]).


% What if the server mangles our questions? (Drops or changes them?)
% We'll lose we'll have no possibility of completing the recurse?
% By replacing returned questions with sent ones, it's possible
% to keep the mapping, and recognize mangled responses.
% Should mangling a request count negatively towards the address rep?
restore_sent_questions(NewMsg = #{'Questions' := NewQuestions}, #{'Questions' := OrigQuestions}) ->
    NewMsg#{'Questions' := OrigQuestions, returned_questions => NewQuestions}.


task_open_questions(#recurse_task{open_questions=Questions}) ->
    Questions.


tried(Msg) ->
    maps:get(tried, Msg, #tried{}).
