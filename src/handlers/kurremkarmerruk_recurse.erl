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
-module(kurremkarmerruk_recurse).

-behavior(gen_server).
-export([init/1,code_change/3,terminate/2,handle_call/3,handle_cast/2,handle_info/2]).

-behavior(kurremkarmerruk_handler).
-export([execute/2,valid_opcodes/0,config_keys/0,config_init/1,handle_config/3,config_end/1]).

-export([
    start_link/0,
    resolve/1,
    resolve/2,
    return_result/2,
    message_timeout/1,
    ack_message_send/2,
    reroute_message_send/3,
    reroute_message/2,
    timeout_pesterer/2,
    address_rep_checker/3,
    update_address_reputation/3,
    address_connected/1,
    address_disconnected/1,
    report_error/2,
    message_requires_tcp/1,
    address_reputation/1,
    address_reputations/0,
    referral_choose_address/2,
    referral_choose_address/3,
    referral_choose_address/4,
    referral_add_reputation_info/1,

    address_family_unavailable/1,

    task_open_questions/1
]).

-include_lib("kurremkarmerruk/include/recurse.hrl").

-define(SIN_LIMIT, 20).
-define(DEFAULT_STRATEGY, {recurse_stub,nil}).
-define(ITERATE_STRATEGY, {recurse_root_iterate, [{cache_glue, true}]}).

valid_opcodes() -> query.
config_keys() -> [recurse].


config_init(Map) ->
    Map#{
        recurse_config => #{
            servers => [],
            recurse_strategy => ?DEFAULT_STRATEGY
        }
    }.


config_end(Map = #{recurse_config := Config}) ->
    case Config of
        #{servers := true} -> Map#{recurse => true, recurse_config => Config#{recurse_strategy => ?ITERATE_STRATEGY}};
        #{servers := false} -> maps:remove(recurse_config, Map#{recurse => false});
        #{servers := []} -> maps:remove(recurse_config, Map#{recurse => false});
        _ -> Map#{recurse => true}
    end.


handle_config(recurse, Config, Map = #{recurse_config := RecurseConfig0}) ->
    RecurseConfig1 = case Config of
        false -> RecurseConfig0#{servers => false};
        true  -> RecurseConfig0#{servers => true};
        [Head|_] = Host when is_integer(Head) -> RecurseConfig0#{servers => [handle_recurse(Host)]};
        Specs when is_list(Specs) ->
            % In this list might be any of the possible forms
            List = lists:flatten(lists:map(fun handle_recurse/1, Specs)),
            RecurseConfig0#{servers => List};
        {[Head|_], _} = Tuple when is_integer(Head) -> RecurseConfig0#{servers => [handle_recurse(Tuple)]};
        {Hosts, Opts} when is_list(Hosts) ->
            Specs = [{Host, Opts} || Host <- Hosts],
            List = lists:flatten(lists:map(fun handle_recurse/1, Specs)),
            RecurseConfig0#{servers => List}
    end,
    {ok, Map#{recurse_config => RecurseConfig1}}.


handle_recurse(Host) when is_list(Host) ->
    % Parameter is "dns-server.com"
    case kurremkarmerruk_utils:parse_host_port(Host, 53) of
        {ok, Spec} -> handle_recurse_final_form(Spec, [])
    end;
handle_recurse({[Head|_] = Host, Opts}) when is_integer(Head), is_list(Opts) ->
    % Parameter is {"dns-server.com", []}
    % Also, port might be replaced in opts.
    case kurremkarmerruk_utils:parse_host_port(Host, default) of
        {ok, Spec} -> handle_recurse_final_form(Spec, Opts)
    end;
handle_recurse({Hosts, Opts}) when is_list(Hosts), is_list(Opts) ->
    % Parameter is {["dns-server.com", "localhost"], []}
    Tls = lists:member(tls, Opts),
    Basic = lists:member(basic, Opts),
    Multiproto = Tls andalso Basic,
    Fn = case Multiproto of
        true ->
            fun
                (Host) when is_list(Host) ->
                    case kurremkarmerruk_utils:parse_host_port(Host, default) of
                        {ok, {_, default}=Tuple} -> Tuple;
                        {ok, {_, {_, default}}=Tuple} -> Tuple
                    end;
                (Address) when is_tuple(Address), tuple_size(Address) =/= 2 -> {Address, default}
            end;
        false ->
            fun
                (Host) when is_list(Host) ->
                    {ok, Tuple} = kurremkarmerruk_utils:parse_host_port(Host, default),
                    Tuple;
                (Address) when is_tuple(Address), tuple_size(Address) =/= 2 -> {Address, default};
                ({_, _}=Tuple) -> Tuple
            end
    end,
    lists:flatten([handle_recurse_final_form(Spec, Opts) || Spec <- lists:map(Fn, Hosts)]);
handle_recurse({Address, Port}=Tuple) when is_tuple(Address), Port > 0, Port < 16#FFFF ->
    handle_recurse_final_form(Tuple, []);
handle_recurse({{Address, Port}=Tuple, Opts}) when is_tuple(Address), is_list(Opts), Port > 0, Port < 16#FFFF ->
    handle_recurse_final_form(Tuple, Opts).


handle_recurse_final_form({Address, Port}, Opts0) ->
    Tls = lists:member(tls, Opts0),
    Basic = lists:member(basic, Opts0) orelse not Tls,
    Opts1 = [GenOpt || GenOpt <- Opts0, GenOpt =/= basic, genOpt =/= tls],
    Fn = fun
        (_, FunPort) when is_integer(FunPort) -> FunPort;
        (tls, default) -> 853;
        (basic, default) -> 53
    end,
    MapList = [Proto || {Proto, _}  <- lists:filter(fun ({_, Enabled}) -> Enabled end, [{tls, Tls}, {basic, Basic}])],
    case lists:map(fun (Proto) -> {{Proto, {Address, Fn(Proto, Port)}}, Opts1} end, MapList) of
        [Tuple] -> Tuple;
        RetList -> RetList
    end.


execute(Msg = #{'Recursion_desired' := false}, _) ->
    {ok, Msg};
execute(Msg, Namespace = #{recurse_config := _}) ->
    {ok, Answers} = resolve(recurse_questions(Msg), [{namespace, Namespace}]),
    cache_results(Namespace, Answers),
    {ok, kurremkarmerruk_handler:add_answers(Answers, false, Msg)};
execute(Msg, _) ->
    {ok, Msg}.


cache_results(Namespace, Results) ->
    % Since recurse will dip into zone and cache, some of the results will be
    % sourced locally and thus require no caching. Need to keep track of that
    case kurremkarmerruk_cache:namespace_cache_enabled(Namespace) of
        true -> kurremkarmerruk_cache:store_interpret_results(Namespace, Results);
        false -> skip
    end.


recurse_questions(Msg = #{'Questions' := Questions}) ->
    recurse_questions(Questions, Msg, []).

recurse_questions([], _, Acc) ->
    Acc;
recurse_questions([Question|Rest], Msg, Acc) ->
    case kurremkarmerruk_handler:current_answers(Question, Msg) of
        [] -> recurse_questions(Rest, Msg, [Question|Acc]);
        Answers ->
            case [GenTuple || GenTuple <- Answers, element(2, GenTuple) =:= referral orelse element(2, GenTuple) =:= addressless_referral] of
                [] -> recurse_questions(Rest, Msg, [Question|Acc]);
                [Referral] -> recurse_questions(Rest, Msg, [Referral|Acc])
            end
    end.


-record(server_state, {tab_msg, tab_timeout, tab_rep, resolvers=#{}, address_rep_ref, address_families, af_sins, namespaces=kurremkarmerruk_server:namespaces_id_keyed()}).
-record(recurse_task, {
    previous_task,
    namespace,
    open_questions,
    answers=[],
    pending_questions=[],
    timeout,
    strategy,
    strategy_state,
    zone_available=false,
    cache_available=false
}).
-record(address_reputation, {
    address,
    latest_latency,
    avg_latency,
    avg_latency_datum_count=0,
    max_latency,
    tried_udp_size=0,
    advertised_udp_size=512,
    created=erlang:monotonic_time(millisecond),
    last_successful_contact,
    last_error_contact,
    requests=0,
    successful=0,
    timeout=0,
    errors=0,
    connected=false,
    blocked_ports=[]
}).


init([]) ->
    process_flag(trap_exit, true),
    TabMsg = ets:new(dns_resolver, []),
    TabTmout = ets:new(dns_resolver_timeout, [ordered_set, public]),
    TabRep = ets:new(dns_resolver_reputation, [named_table, {keypos, 2}, ordered_set]),
    timer:send_interval(timer:minutes(1), refresh_reputations),
    _PesterPid = spawn_link(?MODULE, timeout_pesterer, [self(), TabTmout]),
    Afs = kurremkarmerruk_server:bound_address_families(),
    Sins = maps:from_list([{Af, 0} || Af <- Afs]),
    {ok, #server_state{tab_msg=TabMsg,tab_timeout=TabTmout,tab_rep=TabRep,address_families=Afs,af_sins=Sins}}.


code_change(_OldVsn, _State, _Extra) ->
    {ok, _State}.


terminate(_Reason, _State) ->
    ok.


prepare_resolve({basic, AddressTuple={{_, _, _, _}, _}}, Msg) ->
    {udp_inet, client_udp, #{socket_type => inet}, {AddressTuple, Msg}};
prepare_resolve({basic, AddressTuple={{_, _, _, _, _, _, _, _}, _}}, Msg) ->
    {udp_inet6, client_udp, #{socket_type => inet6}, {AddressTuple, Msg}};
prepare_resolve({Type, AddressTuple={Address, Port}}, Msg) ->
    Cbinfo = case Type of
        tcp -> {gen_tcp,tcp,tcp_closed,tcp_error};
        tls -> {ssl,ssl,ssl_closed,ssl_error}
    end,
    Opts = #{
        address => Address,
        port => Port,
        cbinfo => Cbinfo
    },
    {{Type, AddressTuple}, dns_stream_protocol, Opts, Msg}.


handle_call({recurse_request, Messages, Pid, Ref}, _, State = #server_state{tab_msg=Tab}) ->
    RegisterResult = register_recurse_requests(Messages, Pid, Ref, Tab, []),
    task_resolver(Messages, State), % Should prolly know how busy this or that resolver is (crowded = Prolly a problem)
    case RegisterResult of
        [] -> {reply, ok, State};
        _ -> {reply, {rename, RegisterResult}, State}
    end;
handle_call(available_afs, _, State = #server_state{address_families=Afs,af_sins=Sins}) ->
    {reply, {Afs, Sins}, State};
handle_call(_, _, State) ->
    {noreply, State}.


handle_cast({msg, Msg}, State = #server_state{tab_msg=Tab}) ->
    case ets:take(Tab, dnsmsg:id(Msg)) of
        [] -> ok;
        [{_, Pid, Ref}] -> Pid ! {recurse_response, Ref, Msg}
    end,
    {noreply, State};
handle_cast({timeout, Id}, State = #server_state{tab_msg=Tab}) ->
    case ets:take(Tab, Id) of
        [] -> ok;% It could be that lookup returns nothing, as the message has already received a response
        [{Id, Pid, Ref}] -> Pid ! {recurse_timeout, Ref, Id}
    end,
    {noreply, State};
handle_cast({address_family_unavailable, Af, Pid}, State = #server_state{resolvers=Resolvers0,af_sins=Sins0}) ->
    % Remove resolver to stop messages being sent to it.
    Resolvers1 = case lists:filter(fun ({_, Tuple}) -> element(1, Tuple) =:= Pid end, maps:to_list(Resolvers0)) of
        [] -> Resolvers0;
        [{Key, _}] -> maps:remove(Key, Resolvers0)
    end,
    % Discourage the use of this af
    #{Af := Count0} = Sins0,
    Count1 = case Count0 of
        0 -> 10;
        _ -> Count0 * 2
    end,
    Sins1 = Sins0#{Af := Count1},
    State1 = case lists:all(fun ({_, Value}) -> Value >= ?SIN_LIMIT end, maps:to_list(Sins1)) of
        true -> State#server_state{resolvers=Resolvers1,af_sins=maps:map(fun (_, Value) -> Value rem ?SIN_LIMIT end, Sins1)};
        false -> State#server_state{resolvers=Resolvers1,af_sins=Sins1}
    end,
    {noreply, State1};
handle_cast({reroute_message, Id, Reason}, State = #server_state{tab_msg=Tab}) ->
    case ets:take(Tab, Id) of
        [{_, Pid, Ref}] -> Pid ! {recurse_reroute, Ref, Id, Reason}
    end,
    {noreply, State};
handle_cast({update_address_reputation, Address, Latency, UDPSize}, State = #server_state{tab_rep=Tab}) ->
    % Is it possible that one address family is over and above the better one?
    EtsTuple = case ets:lookup(Tab, Address) of
        [] -> create_address_rep_entry(Tab, Address);
        [Tmp] -> Tmp
    end,
    Fields = address_reputation_fields(),
    true = ets:update_element(Tab, Address, [
        {maps:get(latest_latency, Fields), Latency},
        {maps:get(advertised_udp_size, Fields), UDPSize},
        {maps:get(last_successful_contact, Fields), erlang:monotonic_time(millisecond)},
        {maps:get(successful, Fields), element(maps:get(successful, Fields),EtsTuple)+1}
    ]),
    {noreply, State};
handle_cast({error, Address, Reason}, State = #server_state{tab_rep=Tab}) ->
    EtsTuple = case ets:lookup(Tab, Address) of
        [] -> create_address_rep_entry(Tab, Address);
        [Tmp] -> Tmp
    end,
    Fields = address_reputation_fields(),
    Updates = case Reason of
        format_error -> [{maps:get(errors, Fields), element(maps:get(errors, Fields),EtsTuple)+10}];
        {econnrefused, _Proto, _Port} -> [{maps:get(errors, Fields), element(maps:get(errors, Fields),EtsTuple)+30}];
        missing_responses -> [{maps:get(errors, Fields), element(maps:get(errors, Fields),EtsTuple)+1}];
        unsolicited_message -> [{maps:get(errors, Fields), element(maps:get(errors, Fields),EtsTuple)+1}]
    end,
    true = ets:update_element(Tab, Address, [
        {maps:get(last_error_contact, Fields), erlang:monotonic_time(millisecond)}
        |Updates
    ]),
    {noreply, State};
handle_cast({message_requires_tcp, Id}, State = #server_state{tab_msg=Tab}) ->
    case ets:lookup(Tab, Id) of
        [{Id, Pid, Ref}] -> Pid ! {recurse_requires_tcp, Ref, Id}
    end,
    {noreply, State};
handle_cast({address_timeout, Address, TriedUDPSize}, State) ->
    address_timeout(Address, TriedUDPSize, State),
    {noreply, State};
handle_cast({address_connected, Address}, State = #server_state{tab_rep=Tab}) ->
    _EtsTuple = case ets:lookup(Tab, Address) of
        [] -> create_address_rep_entry(Tab, Address);
        [Tmp] -> Tmp
    end,
    Fields = address_reputation_fields(),
    true = ets:update_element(Tab, Address, [
        {maps:get(connected, Fields), true}
    ]),
    {noreply, State};
handle_cast({address_disconnected, Address}, State = #server_state{tab_rep=Tab}) ->
    _EtsTuple = case ets:lookup(Tab, Address) of
        [] -> create_address_rep_entry(Tab, Address);
        [Tmp] -> Tmp
    end,
    Fields = address_reputation_fields(),
    true = ets:update_element(Tab, Address, [
        {maps:get(connected, Fields), false}
    ]),
    {noreply, State};
handle_cast(_Msg, _State) ->
    {noreply, _State}.


handle_info({'DOWN', Mon, process, Resolver, _}, State) ->
    {noreply, handle_worker_down(Resolver, Mon, State)};
handle_info(refresh_reputations, State = #server_state{tab_rep=Tab,address_rep_ref=undefined,af_sins=Sins0}) ->
    Ref = make_ref(),
    spawn_link(?MODULE, address_rep_checker, [self(), Tab, Ref]),
    Sins1 = maps:map(fun (_, Value) when Value > 0 -> Value - 1; (_, Value) -> Value end, Sins0),
    {noreply, State#server_state{address_rep_ref=Ref,af_sins=Sins1}};
handle_info({address_rep_results, Ref, Drop, Decrease}, State = #server_state{tab_rep=Tab,address_rep_ref=Ref}) ->
    Fields = address_reputation_fields(),
    [ets:delete(Tab, GenId) || GenId <- Drop],
    UpdateCounter = [{maps:get(errors, Fields), -1, 0, 0}, {maps:get(timeout, Fields), -1, 0, 0}],
    [ets:update_counter(Tab, GenId, UpdateCounter) || GenId <- Decrease],
    {noreply, State#server_state{address_rep_ref=undefined}};
handle_info(_Msg, _State) ->
    {noreply, _State}.


start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


resolve(Query) ->
    resolve(Query, []).


%-spec resolve([dnslib:question()] | dnslib:question(), map()) -> list().
resolve({[Head|_]=Domain0, Type, Class}, Opts) when is_integer(Head) ->
    {_, false, Domain} = dnslib:list_to_domain(Domain0),
    resolve({Domain, Type, Class}, Opts);
resolve(Query, Opts) when is_list(Opts), is_tuple(Query) ->
    resolve([Query], Opts);
resolve(Query, Opts) when is_list(Opts) ->
    resolve_init(Query, Opts).


resolve_init(Query, Opts) ->
    Task0 = #recurse_task{open_questions=Query},
    {Namespace, _} = case proplists:get_value(namespace, Opts) of
        undefined -> {undefined, undefined};
        CaseSpace -> {CaseSpace, maps:get(namespace_id, CaseSpace)}
    end,
    Task1 = case Namespace of
        undefined -> Task0;
        _ ->
            Zone = kurremkarmerruk_handler:is_preceeded_by(?MODULE, kurremkarmerruk_zone, Namespace),
            Cache = kurremkarmerruk_handler:is_preceeded_by(?MODULE, kurremkarmerruk_zone, Namespace),
            Task0#recurse_task{cache_available=Cache,zone_available=Zone}
    end,
    {{RecurseMod, InitArgs}, Config} = case Namespace of
        undefined -> {?ITERATE_STRATEGY, true}; % Should the default strategy be configurable?
        #{recurse_config := RecurseConfig} -> {maps:get(recurse_strategy, RecurseConfig), maps:get(servers, RecurseConfig)}
    end,
    {ok, Actions, RecurseState} = RecurseMod:init(Query, Config, available_afs(), InitArgs),
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

send_routed(Messages, Ref) ->
    gen_server:call(?MODULE, {recurse_request, Messages, self(), Ref}).


resolve_wait_responses(Task, [], _) ->
    resolve_end(Task);
resolve_wait_responses(Task, Messages, Ref) ->
    receive
        % If we want to try saving recursed requests,
        % we might want to track sent questions and
        % refer one recursing process to another.
        % Then as the one doing the resolving is done,
        % it can send the answers it received to the other.
        % Suppose this assumes that both are in the same namespace.
        % Add a field to record for pending pids.
        {recurse_response, Ref, Message} ->
            Id = dnsmsg:id(Message),
            {[{_, Original}], Rest} = lists:partition(fun ({_, FunMsg}) -> dnsmsg:id(FunMsg) =:= Id end, Messages),
            resolve_handle_response(Message, Original, Task, Rest, Ref);
        {recurse_timeout, Ref, Id} ->
            {[{_, Timeouted}], Rest} = lists:partition(fun ({_, FunMsg}) -> dnsmsg:id(FunMsg) =:= Id end, Messages),
            #{tried := Tried} = Timeouted,
            #tried{servers=[{{_, {Address, _}}=ServerSpec, nil}|Prev]} = Tried,
            address_timeout(Address, dnsmsg:udp_payload_max_size(Timeouted)),
            resolve_reroute_message(Timeouted#{tried => Tried#tried{servers=[{ServerSpec, timeout}|Prev]}}, Task, Rest, Ref);
        {recurse_reroute, Ref, Id, Reason} ->
            {[{_, Rerouted}], Rest} = lists:partition(fun ({_, FunMsg}) -> dnsmsg:id(FunMsg) =:= Id end, Messages),
            #{tried := Tried} = Rerouted,
            #tried{servers=[{{_, {Address, _}}=ServerSpec, nil}|Prev],afs=TriedAfs} = Tried,
            Tried1 = case Reason of
                bad_af when tuple_size(Address) =:= 4 -> Tried#tried{servers=[{ServerSpec, Reason}|Prev], afs=[inet|TriedAfs]};
                bad_af when tuple_size(Address) =:= 8 -> Tried#tried{servers=[{ServerSpec, Reason}|Prev], afs=[inet6|TriedAfs]};
                _ -> Tried#tried{servers=[{ServerSpec, Reason}|Prev]}
            end,
            resolve_reroute_message(Rerouted#{tried => Tried1}, Task, Rest, Ref);
        {recurse_requires_tcp, Ref, Id} ->
            {[{_, Message}], Rest} = lists:partition(fun ({_, FunMsg}) -> dnsmsg:id(FunMsg) =:= Id end, Messages),
            #{tried := Tried, 'Questions' := [Question]} = Message,
            #tried{servers=[{{basic, AddressPort}, _}|_]} = Tried,
            ServerSpec = {tcp, AddressPort},
            {Chain, Task1} = chain_for_message(Message, Task),
            resolve_handle_actions([{route_question, {#{}, Question}, ServerSpec, Chain, Tried}], Task1, Rest, Ref)
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
    #tried{servers=[{{basic, AddressPort}, _}|_]} = Tried,
    ServerSpec = {tcp, AddressPort},
    {Chain, Task1} = chain_for_message(Original, Task0),
    resolve_handle_actions([{route_question, {#{}, Question}, ServerSpec, Chain, Tried}], Task1, Rest, Ref);
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
    {ok, kurremkarmerruk_utils:limit_answer_ttls(Answers, MaxTtl, MaxNegTtl, [])}.


return_result(Pid, Msg) ->
    gen_server:cast(Pid, {msg, Msg}).


message_timeout(Id) ->
    gen_server:cast(?MODULE, {timeout, Id}).


address_rep_checker(Pid, Tab, Ref) ->
    % Should somehow get rid of this table walk...
    % Triered system, where active and good quality addresses are preserved?
    TimeSinceContact = application:get_env(kurremkarmerruk, address_reputation_max_age, 1800),
    address_rep_checker([], [], ets:first(Tab), erlang:monotonic_time(millisecond), timer:seconds(TimeSinceContact), Pid, Tab, Ref).

address_rep_checker(Drop, Keep, '$end_of_table', _, _, Pid, _, Ref) ->
    Pid ! {address_rep_results, Ref, Drop, Keep},
    exit(normal);
address_rep_checker(Drop, Keep, Key, Now, MaxAge, Pid, Tab, Ref) ->
    case ets:lookup(Tab, Key) of
        [] -> address_rep_checker(Drop, Keep, ets:next(Tab, Key), Now, MaxAge, Pid, Tab, Ref);
        [#address_reputation{last_successful_contact=undefined,created=Created}] when Now - Created >= MaxAge ->
            address_rep_checker([Key|Drop], Keep, ets:next(Tab, Key), Now, MaxAge, Pid, Tab, Ref);
        [#address_reputation{last_successful_contact=Last}] when Now - Last >= MaxAge ->
            address_rep_checker([Key|Drop], Keep, ets:next(Tab, Key), Now, MaxAge, Pid, Tab, Ref);
        [#address_reputation{timeout=0,errors=0}] -> address_rep_checker(Drop, Keep, ets:next(Tab, Key), Now, MaxAge, Pid, Tab, Ref);
        _ -> address_rep_checker(Drop, [Key|Keep], ets:next(Tab, Key), Now, MaxAge, Pid, Tab, Ref)
    end.


register_recurse_requests([], _, _, _, Acc) ->
    Acc;
register_recurse_requests([{_, Message}|Rest], Pid, Ref, Tab, Acc) ->
    case ets:insert_new(Tab, {dnsmsg:id(Message), Pid, Ref}) of
        false ->
            case Message of
                #{orig_id := _} -> register_recurse_requests([{nil, dnsmsg:reset_id(Message)}|Rest], Pid, Ref, Tab, Acc);
                #{} -> register_recurse_requests([{nil, dnsmsg:reset_id(Message#{old_id => dnsmsg:id(Message)})}|Rest], Pid, Ref, Tab, Acc)
            end;
        true ->
            case Message of
                #{orig_id := OrigId} -> register_recurse_requests(Rest, Pid, Ref, Tab, [{OrigId, dnsmsg:id(Message)}|Acc]);
                #{} -> register_recurse_requests(Rest, Pid, Ref, Tab, Acc)
            end
    end.


task_resolver([], State) ->
    {ok, State};
task_resolver(All=[{ServerSpec = {_, {Address, _}}, Msg0}|Rest], State = #server_state{resolvers=Resolvers,tab_timeout=TabTmout}) ->
    {Key, Module, Opts, Msg1} = prepare_resolve(ServerSpec, Msg0#{timeout => 500}),
    case Resolvers of
        #{Key := {Pid, Mon}} ->
            Ref = make_ref(),
            Pid ! {send, self(), Ref, Msg1, 500},
            receive
                {'DOWN', Mon, process, Pid, _} -> task_resolver(All, handle_worker_down(Pid, Mon, State));
                {send_ack, Pid, Ref} ->
                    Timeout = erlang:monotonic_time(microsecond) + 1000000, % 1 second
                    true = ets:insert_new(TabTmout, {Timeout, dnsmsg:id(Msg0)}),
                    inc_messages_sent(Address, State),
                    task_resolver(Rest, State);
                {send_reroute, Pid, Ref, Reason} ->
                    reroute_message(Msg0, Reason),
                    task_resolver(Rest, State)
            after
                1000 ->
                    exit(Pid, kill),
                    receive
                        {'DOWN', Mon, process, Pid, _} -> task_resolver(All, handle_worker_down(Pid, Mon, State))
                    after
                        250 -> error(task_resolver_kill)
                    end
            end;
        #{} ->
            {ok, Resolver} = supervisor:start_child(client_connection_sup, [Module, self(), Opts]),
            Mon = monitor(process, Resolver), % What if there's a loop of worker spawns-downs for some reason?
            task_resolver(All, State#server_state{resolvers=Resolvers#{Key => {Resolver, Mon}}})
    end.


handle_recursed_actions([], Task, Acc) ->
    {Acc, Task};
handle_recursed_actions([{close_question, Answer}|Rest], Task = #recurse_task{answers=Answers,open_questions=Questions}, Acc) ->
    Question = element(1, Answer),
    handle_recursed_actions(Rest, Task#recurse_task{answers=[Answer|Answers],open_questions=lists:delete(dnslib:normalize_question(Question), Questions)}, Acc);
handle_recursed_actions([{ActionType, Args, Server, Chain}|Rest], Task, Acc) ->
    handle_recursed_actions([{ActionType, Args, Server, Chain, #tried{}}|Rest], Task, Acc);
handle_recursed_actions([{route_question, Args, _, _, #tried{}}=Action|Rest], Task, Acc) when tuple_size(Args) >= 2 ->
    handle_recursed_route_question_precheck(Action, Rest, Task, Acc);
handle_recursed_actions([{route_question_skip_precheck, Args, _, _, #tried{}}=Action|Rest], Task, Acc) when tuple_size(Args) >= 2 ->
    handle_recursed_route_question(Action, Rest, Task, Acc);
handle_recursed_actions([{ActionType, Args, Server, Chain, TriedServers}|Rest], Task, Acc) ->
    handle_recursed_actions([{ActionType, Args, Server, Chain, #tried{servers=TriedServers}}|Rest], Task, Acc);
handle_recursed_actions([{ActionType, Args, Server, Chain, TriedServers, TriedQueries}|Rest], Task, Acc) ->
    handle_recursed_actions([{ActionType, Args, Server, Chain, #tried{servers=TriedServers,queries=TriedQueries}}|Rest], Task, Acc).


handle_recursed_route_question_precheck(Action, Rest, Task = #recurse_task{namespace=Namespace,zone_available=Zone,cache_available=Cache}, Acc) ->
    Question = element(2, element(2, Action)),
    case Question of
        [] -> error(no_questions);
        _ -> ok
    end,
    case [GenPrev || {true, GenPrev} <- [{Zone, kurremkarmerruk_zone}, {Cache, kurremkarmerruk_cache}]] of
        [] -> handle_recursed_route_question(Action, Rest, Task, Acc);
        Previous ->
            % execute_query for both, figure out if either produced an answer to the question.
            Question = element(2, element(2, Action)),
            case got_answer(lists:foldl(fun (FunMod, FunAcc) -> lists:append(FunMod:execute_query(Question, Namespace), FunAcc) end, [], Previous)) of
                false -> handle_recursed_route_question(Action, Rest, Task, Acc);
                {true, Answer} ->
                    #recurse_task{
                        strategy=RecurseMod,
                        strategy_state=RecurseState0
                    } = Task,
                    % Since we already have an answer, we can submit a chain without referrals
                    Chain = [GenTuple || GenTuple <- element(4, Action), element(2, GenTuple) =/= referral, element(2, GenTuple) =/= addressless_referral],
                    % There's no need to match pending, as we already have possible chain in
                    % action, and nothing has been inserted in state yet.
                    %{Matched, Pending1} = match_answers_to_pending([Answer], Pending0, []),
                    {ok, FurtherActions, RecurseState1} = RecurseMod:response([{Answer, Chain}], nil, available_afs(), element(5, Action), RecurseState0),
                    Task1 = Task#recurse_task{strategy_state=RecurseState1},
                    {NewMessages, Task2} = handle_recursed_actions(FurtherActions, Task1, []),
                    handle_recursed_actions(Rest, Task2, lists:append(NewMessages, Acc))
            end
    end.


got_answer([]) ->
    false;
got_answer([Answer|Rest]) when tuple_size(Answer) =:= 2 ->
    % This case is more or less specifically for zone.
    % Is there a better/more clear way to do this?
    got_answer(Rest);
got_answer([Answer|_]) when tuple_size(Answer) =/= 2 ->
    {true, Answer}.


handle_recursed_route_question({_, Args, Server, Chain, Tried}, Rest, Task = #recurse_task{pending_questions=Pending}, Acc) ->
    Question = element(2, Args),
    #tried{servers=TriedServers, queries=TriedQueries} = Tried,
    Message0 = apply(dnsmsg, new, tuple_to_list(Args)),
    Message1 = Message0#{
        tried => Tried#tried{servers=[{Server, nil}|TriedServers], queries=[Question|TriedQueries]}
    },
    handle_recursed_actions(Rest, Task#recurse_task{pending_questions=[{dnslib:normalize_question(Question), Chain}|Pending]}, [{Server, Message1}|Acc]).


handle_worker_down(Pid, Ref, State = #server_state{resolvers=Resolvers0}) ->
    Resolvers1 = case lists:filter(fun ({_, Tuple}) -> Tuple =:= {Pid, Ref} end, maps:to_list(Resolvers0)) of
        [] -> Resolvers0;
        [{Key, _}] -> maps:remove(Key, Resolvers0)
    end,
    State#server_state{resolvers=Resolvers1}.


ack_message_send(Pid, Ref) ->
    Pid ! {send_ack, self(), Ref}.


reroute_message_send(Pid, Ref, Reason) ->
    Pid ! {send_reroute, self(), Ref, Reason}.


address_family_unavailable(Af) ->
    gen_server:cast(?MODULE, {address_family_unavailable, Af, self()}).


reroute_message(Msg, Reason) when is_map(Msg) ->
    gen_server:cast(?MODULE, {reroute_message, dnsmsg:id(Msg), Reason});
reroute_message(Id, Reason) when is_integer(Id) ->
    gen_server:cast(?MODULE, {reroute_message, Id, Reason}).


update_address_reputation(Address, Latency, UDPSize) ->
    gen_server:cast(?MODULE, {update_address_reputation, Address, Latency, UDPSize}).


address_connected(Address) ->
    gen_server:cast(?MODULE, {address_connected, Address}).

address_disconnected(Address) ->
    gen_server:cast(?MODULE, {address_disconnected, Address}).


report_error(Address, Reason) ->
    gen_server:cast(?MODULE, {error, Address, Reason}).


message_requires_tcp(Id) ->
    gen_server:cast(?MODULE, {message_requires_tcp, Id}).


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


timeout_pesterer(Pid, Tab) ->
    receive after 100 ->
        Now = erlang:monotonic_time(microsecond),
        case ets:select(Tab, [{{'$1', '_'}, [{'=<', '$1', Now}], ['$_']}]) of
            [] -> timeout_pesterer(Pid, Tab);
            Timeouts ->
                lists:foreach(fun ({Timeout, Id}) -> true = ets:delete(Tab, Timeout), message_timeout(Id) end, Timeouts),
                timeout_pesterer(Pid, Tab)
        end
    end.


task_open_questions(#recurse_task{open_questions=Questions}) ->
    Questions.


available_afs() ->
    {Afs, Sins} = gen_server:call(?MODULE, available_afs),
    available_afs(Afs, Sins).

available_afs(Afs, Sins) ->
    List = lists:sort(fun (Af1, Af2) -> maps:get(Af1, Sins) < maps:get(Af2, Sins) end, Afs),
    [GenAf || GenAf <- List, maps:get(GenAf, Sins) < ?SIN_LIMIT].


tried(Msg) ->
    maps:get(tried, Msg, #tried{}).


address_reputation(Address) ->
    case ets:lookup(dns_resolver_reputation, Address) of
        [] -> unknown;
        [Tuple] -> Tuple
    end.


address_reputations() ->
    ets:tab2list(dns_resolver_reputation).


address_timeout(Address, TriedUDPSize) ->
    gen_server:cast(?MODULE, {address_timeout, Address, TriedUDPSize}).

address_timeout(Address, _TriedUDPSize, #server_state{tab_rep=Tab}) ->
    Fields = address_reputation_fields(),
    ets:update_counter(Tab, Address, {maps:get(timeout, Fields), 1}).


inc_messages_sent(Address, #server_state{tab_rep=Tab}) ->
    EtsTuple = case ets:lookup(Tab, Address) of
        [] -> create_address_rep_entry(Tab, Address);
        [Tmp] -> Tmp
    end,
    Fields = address_reputation_fields(),
    true = ets:update_element(Tab, Address, [
        {maps:get(requests, Fields), element(maps:get(requests, Fields),EtsTuple)+1}
    ]).


create_address_rep_entry(Tab, Address) ->
    Default = #address_reputation{address=Address},
    true = ets:insert_new(Tab, Default),
    Default.


referral_choose_address(Referral, Af) ->
    referral_choose_address(Referral, Af, #tried{}, none).

referral_choose_address(Referral, Af, Tried) ->
    referral_choose_address(Referral, Af, Tried, none).

referral_choose_address({{_, referral, NsAddressRrs}, referral_with_reputation_info, _}, [], #tried{}, _) ->
    case [GenNsRr || {GenNsRr, []} <- NsAddressRrs] of
        [] -> {error, no_suitable_address};
        NsRrs -> {addressless_nsrrs, NsRrs}
    end;
referral_choose_address({_, referral_with_reputation_info, AddressReps0}=Referral, [Af|OtherAf], Tried = #tried{servers=TriedServers,afs=TriedAfs}, ErrorTolerance) ->
    % Should choose from any of the offered addresses, not just the first ns...
    % We can't currently distinguish between tls and basic, although tried does contain that
    % information. We should be able to fail from tls to basic (if such option is provided)
    % Allow ServerSpec in referral addresses? Though we currently keep reputation
    % keyed only by address...
    case lists:member(Af, TriedAfs) of
        true -> referral_choose_address(Referral, OtherAf, Tried, ErrorTolerance);
        false ->
            TupleSize = case Af of
                inet -> 4;
                inet6 -> 8
            end,
            TriedAddresses = [GenAddress || {{_, {GenAddress, _}}, _} <- TriedServers],
            MaxErrors = case ErrorTolerance of
                none -> 0
            end,
            AddressReps = [GenTuple || {GenAddress, _}=GenTuple <- AddressReps0, tuple_size(GenAddress) =:= TupleSize, not lists:member(GenAddress, TriedAddresses)],
            Total = length(AddressReps),
            case
                % Other option is to consider whether we already know a reasonably low latency option (What is low enough should prolly be configurable)
                % If we have, don't bother contacting other servers
                % Or if we only know servers with unacceptably long latencies, try to contact new ones.
                % Or try to find fast addresses for active servers
                case lists:partition(fun ({_, FunRep}) -> FunRep =/= unknown end, AddressReps) of
                    {[], []} -> [];
                    {_, Unknown} when Total < 10, Unknown =/= [] -> Unknown;
                    {Known, Unknown} when length(Known) < length(Unknown) -> Unknown;
                    {Known, Unknown} when length(Known) > length(Unknown) -> Known
                end
            of
                [] -> referral_choose_address(Referral, OtherAf, Tried, ErrorTolerance);
                [{_, Rep}|_] = ChooseFrom when Rep =:= unknown ->
                    {Address, _} = lists:nth(rand:uniform(length(ChooseFrom)), ChooseFrom),
                    {ok, Address};
                ChooseFrom0 ->
                    % Strong prefer connected?
                    case [GenTuple || {_, #address_reputation{errors=GenErrors}}=GenTuple <- ChooseFrom0, GenErrors =< MaxErrors] of
                        [] -> referral_choose_address(Referral, OtherAf, Tried, ErrorTolerance);
                        ChooseFrom ->
                            % Spread out requests, or choose one with best latency?
                            [{Address, _}|_] = lists:sort(fun ({_, #address_reputation{latest_latency=L1}}, {_, #address_reputation{latest_latency=L2}}) -> L1 < L2 end, ChooseFrom),
                            {ok, Address}
                    end
            end
    end;
referral_choose_address({_, referral, _}=Referral, Af, Tried, ErrorTolerance) ->
    referral_choose_address(referral_add_reputation_info(Referral), Af, Tried, ErrorTolerance).


referral_add_reputation_info({_, referral, NsAddressRrs}=Referral) ->
    AddressRrs = lists:foldl(fun ({_, FunAddressRrs}, FunAcc) -> lists:append(FunAddressRrs, FunAcc) end, [], NsAddressRrs),
    {Referral, referral_with_reputation_info, [{Address, address_reputation(Address)} || {_, _, _, _, Address} <- AddressRrs]}.


address_reputation_fields() ->
    {List, _} = lists:mapfoldl(fun (FunField, Index) -> {{FunField, Index}, Index+1} end, 2, record_info(fields, address_reputation)),
    maps:from_list(List).
