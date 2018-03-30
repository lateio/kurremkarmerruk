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
-module(kurremkarmerruk_cache).
-behavior(kurremkarmerruk_handler).
-export([execute/2,valid_opcodes/0,config_keys/0,config_init/1,handle_config/3,config_end/1]).

-behavior(gen_server).
-export([init/1,code_change/3,terminate/2,handle_call/3,handle_cast/2,handle_info/2]).

-export([
    namespace_cache_enabled/1,
    start_link/0,
    store_resources/2,
    %store_negative/2,
    store_interpret_results/2,
    lookup_resource/2,
    %lookup_ns/2,
    cache_namespace/1,
    cache_invalidator/3,
    execute_query/2
]).

-include_lib("dnslib/include/dnslib.hrl").

valid_opcodes() -> query.
config_keys() -> [cache].

config_init(Map) ->
    Map#{
        cache_config => default_cache_config()
    }.


default_cache_config() ->
    #{
        negative_caching => true,
        opts => [],
        private => false,
        cache_id => default
    }.


config_end(Map = #{cache_config := Config}) ->
    case Config of
        #{opts := false} -> maps:remove(cache_config, Map);
        #{opts := true} -> Map; % Use defaults...
        #{} -> Map
    end.


handle_config(cache, Config, Map = #{cache_config := CacheConfig0}) ->
    CacheConfig1 = case Config of
        false -> CacheConfig0#{opts => false};
        true -> CacheConfig0#{opts => true}
        %{_, Opts}=Tuple when is_list(Opts) -> {ok, Map#{cache => true, cache_config => Tuple}}
    end,
    {ok, Map#{cache_config => CacheConfig1}}.


namespace_cache_enabled(#{cache_config := _}) -> true;
namespace_cache_enabled(_) -> false.


execute(Msg = #{'Questions' := Questions}, Namespace = #{cache_config := _}) ->
    Answers = execute_query(Questions, Namespace),
    {ok, kurremkarmerruk_handler:add_answers(Answers, false, Msg)};
execute(Msg, _) ->
    {ok, Msg}.


execute_query(Query, Namespace = #{cache_config := #{cache_id := Id}}) when is_list(Query) ->
    Ret = check_cache(Query, Id, Namespace, kurremkarmerruk_handler:is_preceeded_by(?MODULE, kurremkarmerruk_zone, Namespace), []),
    Ret;
execute_query(Query, Namespace) when is_tuple(Query) ->
    execute_query([Query], Namespace).


check_cache([], _, Namespace, _, Acc) ->
    case kurremkarmerruk_zone:namespace_zone_enabled(Namespace) of
        true -> kurremkarmerruk_zone:override_authoritative(Acc, Namespace, []);
        false -> Acc
    end;
check_cache([Entry|Questions], NSID, Namespace, ZoneAvailable, Acc0) ->
    % Check if Entry type is actually cacheable
    % What if Entry type all/maila? Or class is any?
    Acc1 = case lookup_resource(NSID, Entry) of
        [] -> Acc0;
        {{name_error, Soa}, CaseAnswers} ->
            [{Entry, name_error, {Soa, CaseAnswers}}|Acc0];
        {{nodata, Soa}, CaseAnswers} ->
            [{Entry, nodata, {Soa, CaseAnswers}}|Acc0];
        Cached ->
            Type = element(2, Entry),
            Tuple = case produce_cache_return(Entry, Cached) of
                {_, cname, {CnameRr, CacheResources}}=Res when ZoneAvailable, Type =/= cname ->
                    CnameDomain = element(5, CnameRr),
                    Class = element(3, Entry),
                    case kurremkarmerruk_zone:execute_query({CnameDomain, Type, Class}, Namespace) of
                        [] -> Res;
                        [{_, _}] -> Res;
                        [{_, ok, ZoneResources}] -> {Entry, ok, lists:append(ZoneResources, CacheResources)};
                        [{_, name_error, {Soa, ZoneResources}}] -> {Entry, name_error, {Soa, lists:append(ZoneResources, CacheResources)}};
                        [{_, nodata, {Soa, ZoneResources}}] -> {Entry, name_error, {Soa, lists:append(ZoneResources, CacheResources)}};
                        [{_, cname, {ZoneCnameRr, ZoneResources}}] -> {Entry, cname, {ZoneCnameRr, lists:append(ZoneResources, CacheResources)}};
                        [{_, cname_loop, ZoneResources}] -> {Entry, cname_loop, lists:append(ZoneResources, CacheResources)};
                        [{_, cname_referral, {ZoneCnameRr, ZoneReferral, ZoneResources}}] -> {Entry, cname_referral, {ZoneCnameRr, ZoneReferral, lists:append(ZoneResources, CacheResources)}}
                    end;
                Res -> Res
            end,
            check_cache(Questions, NSID, Namespace, ZoneAvailable, [Tuple|Acc0])
    end,
    check_cache(Questions, NSID, Namespace, ZoneAvailable, Acc1).


% Referrals?
produce_cache_return({_, cname, _}=Question, [{_, cname, _, _, _}|_]=Resources) ->
    {Question, ok, Resources};
produce_cache_return(Question, [{_, cname, _, _, _}=CnameRr|_]=Resources) ->
    {Question, cname, {CnameRr, Resources}};
produce_cache_return(Question, Resources) ->
    {Question, ok, Resources}.


%%
%%
%%

-record(server_state, {tab, trie=dnstrie:new(), invalidator_ref, authority_cache_refresh_timer}).


init([]) ->
    process_flag(trap_exit, true),
    Tab = ets:new(dns_cache, [named_table, ordered_set]),
    timer:send_interval(timer:minutes(1), invalidate_cache),
    Namespaces = kurremkarmerruk_server:namespaces(),
    State = init_authority_root(#server_state{tab=Tab}),
    {ok, init_namespace_caches(maps:to_list(Namespaces), State)}.


code_change(_OldVsn, _State, _Extra) ->
    {ok, _State}.


terminate(_Reason, _State) ->
    ok.


handle_call({store, NamespaceId, Entries}, _, State = #server_state{tab=Tab, trie=Trie}) ->
    Ttl = case maps:get(NamespaceId, kurremkarmerruk_server:namespaces_id_keyed(), undefined) of
        undefined -> 86400; % 1 Day
        Namespace -> maps:get(max_ttl, Namespace)
    end,
    {reply, ok, State#server_state{trie=store_resources(Entries, namespace:id(NamespaceId), Tab, Trie, Ttl)}};
handle_call({store_interpret_result, NamespaceId, Results}, _, State = #server_state{tab=Tab, trie=Trie}) ->
    {Ttl, TtlNeg} = case maps:get(NamespaceId, kurremkarmerruk_server:namespaces_id_keyed(), undefined) of
        undefined -> {86400, 86400};
        Namespace -> {maps:get(max_ttl, Namespace), maps:get(max_negative_ttl, Namespace)}
    end,
    {reply, ok, State#server_state{trie=store_interpret_results(Results, namespace:id(NamespaceId), Tab, Trie, Ttl, TtlNeg)}};
handle_call(_Msg, _From, _State) ->
    {noreply, _State}.

handle_cast(_Msg, _State) ->
    {noreply, _State}.

handle_info(refresh_authority_cache, State) ->
    {noreply, init_authority_root(State)};
handle_info(invalidate_cache, State = #server_state{tab=Tab,invalidator_ref=undefined}) ->
    Ref = make_ref(),
    spawn_link(?MODULE, cache_invalidator, [self(), Ref, Tab]),
    {noreply, State#server_state{invalidator_ref=Ref}};
handle_info({cache_invalidator_res, Ref, Drop, Check}, State = #server_state{tab=Tab,invalidator_ref=Ref}) ->
    % How would we know to apply cache policies for invalidations?
    % Cache entries should contain some indication of the cache
    % they belong to...
    [ets:delete(Tab, GenKey) || GenKey <- Drop],
    cache_invalidate_check(Check, Tab, nowts()),
    {noreply, State#server_state{invalidator_ref=undefined}};
handle_info(_Msg, _State) ->
    {noreply, _State}.


start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


cache_invalidator(Pid, Ref, Tab) ->
    Now = nowts(),
    cache_invalidator([], [], ets:first(Tab), Now, Pid, Tab, Ref).

cache_invalidator(Drop, Check, '$end_of_table', _, Pid, _, Ref) ->
    Pid ! {cache_invalidator_res, Ref, Drop, Check},
    exit(normal);
cache_invalidator(Drop, Check, Key, Now, Pid, Tab, Ref) ->
    case ets:lookup(Tab, Key) of
        [] -> cache_invalidator(Drop, Check, ets:next(Tab, Key), Now, Pid, Tab, Ref);
        [{Key, ValidUntil, _}] when ValidUntil < Now -> cache_invalidator([Key|Drop], Check, ets:next(Tab, Key), Now, Pid, Tab, Ref);
        [{Key, _, Map}] ->
            case invalidator_check(Map, Now) of
                true -> cache_invalidator(Drop, [Key|Check], ets:next(Tab, Key), Now, Pid, Tab, Ref);
                false -> cache_invalidator(Drop, Check, ets:next(Tab, Key), Now, Pid, Tab, Ref)
            end
    end.


invalidator_check(Map, Now) ->
    Classes = [GenValue || {_, GenValue} <- maps:to_list(Map)],
    invalidator_check_class(Classes, Now).

invalidator_check_class([], _) ->
    false;
invalidator_check_class([{name_error, ValidUntil, _}|Rest], Now) ->
    case Now > ValidUntil of
        true -> true;
        false -> invalidator_check_class(Rest, Now)
    end;
invalidator_check_class([Class|Rest], Now) ->
    case invalidator_check_types([GenValue || {_, GenValue} <- maps:to_list(Class)], Now) of
        false -> invalidator_check_class(Rest, Now);
        true -> true
    end.

invalidator_check_types([], _) ->
    false;
invalidator_check_types([{nodata, ValidUntil, _}|Rest], Now) ->
    case Now > ValidUntil of
        true -> true;
        false -> invalidator_check_types(Rest, Now)
    end;
invalidator_check_types([Rrs|Rest], Now) ->
    case [GenTuple || {_, _, _, ValidUntil, _}=GenTuple <- Rrs, Now > ValidUntil] of
        [] -> invalidator_check_types(Rest, Now);
        _ -> true
    end.

cache_invalidate_check([], _, _) ->
    ok;
cache_invalidate_check([Key|Rest], Tab, Now) ->
    case ets:lookup(Tab, Key) of
        [] -> cache_invalidate_check(Rest, Tab, Now);
        [{Key, _, Map0}] ->
            Map = prune_old(Map0, Now),
            true = ets:update_element(Tab, Key, {3, Map}),
            cache_invalidate_check(Rest, Tab, Now)
    end.

prune_old(Map, Now) ->
    maps:from_list(prune_old_classes(maps:to_list(Map), Now, [])).

prune_old_classes([], _, Acc) ->
    Acc;
prune_old_classes([{_, {name_error, ValidUntil, _}}=Entry|Rest], Now, Acc) ->
    case Now > ValidUntil of
        true -> prune_old_classes(Rest, Now, Acc);
        false -> prune_old_classes(Rest, Now, [Entry|Acc])
    end;
prune_old_classes([{Class, Map0}|Rest], Now, Acc) ->
    Map = maps:from_list(prune_old_types(maps:to_list(Map0), Now, [])),
    prune_old_classes(Rest, Now, [{Class, Map}|Acc]).

prune_old_types([], _, Acc) ->
    Acc;
prune_old_types([{_, {nodata, ValidUntil, _}}=Entry|Rest], Now, Acc) ->
    case Now > ValidUntil of
        true -> prune_old_types(Rest, Now, Acc);
        false -> prune_old_types(Rest, Now, [Entry|Acc])
    end;
prune_old_types([{Type, TypeRrs0}|Rest], Now, Acc) ->
    TypeRrs = [GenTuple || {_, _, _, ValidUntil, _}=GenTuple <- TypeRrs0, Now < ValidUntil],
    prune_old_types(Rest, Now, [{Type, TypeRrs}|Acc]).


store_resources(Namespace = #{cache_config := #{cache_id := Id}}, Resources0) when is_list(Resources0) ->
    Resources = case kurremkarmerruk_zone:namespace_zone_enabled(Namespace) of
        true -> [GenTuple || GenTuple <- Resources0, not kurremkarmerruk_zone:authoritative_for(element(1, GenTuple))];
        false -> Resources0
    end,
    gen_server:call(?MODULE, {store, Id, Resources});
store_resources(NamespaceId, Resources) when is_list(Resources), is_atom(NamespaceId) ->
    gen_server:call(?MODULE, {store, NamespaceId, Resources});
store_resources(Namespace, Resource) ->
    store_resources(Namespace, [Resource]).


store_interpret_results(Namespace = #{cache_config := #{cache_id := Id}}, Results0) when is_list(Results0) ->
    Results = case kurremkarmerruk_zone:namespace_zone_enabled(Namespace) of
        true -> prune_authoritative_from_results(Results0, Namespace, []);
        false -> Results0
    end,
    gen_server:call(?MODULE, {store_interpret_result, Id, Results});
store_interpret_results(NamespaceId, Results) when is_list(Results), is_atom(NamespaceId) ->
    gen_server:call(?MODULE, {store_interpret_result, NamespaceId, Results});
store_interpret_results(Namespace, Result) ->
    store_interpret_results(Namespace, [Result]).


prune_authoritative_from_results([], _, Acc) ->
    Acc;
prune_authoritative_from_results([Tuple|Rest], Namespace, Acc) ->
    % This is awful. Should come up with a consistent way of handling cases like this.
    % But is pruning authoritative needed anywhere else?
    case Tuple of
        {_, ok, Resources} -> prune_authoritative_from_results(Rest, Namespace, [setelement(3, Tuple, [GenTuple || GenTuple <- Resources, not kurremkarmerruk_zone:authoritative_for(element(1, GenTuple), Namespace)])|Acc]);
        {Question, TupleState, {SoaRr, Resources}} when TupleState =:= nodata; TupleState =:= name_error ->
            case Resources of
                [] ->
                    case kurremkarmerruk_zone:authoritative_for(element(1, Question), Namespace) of
                        true -> prune_authoritative_from_results(Rest, Namespace, Acc);
                        false -> prune_authoritative_from_results(Rest, Namespace, [Tuple|Acc])
                    end;
                [{_, cname, _, _, CanonDomain}|ResourcesTail] ->
                    case kurremkarmerruk_zone:authoritative_for(CanonDomain, Namespace) of
                        true -> % Make answer into an ok, drop canon and prune others
                            Tuple1 = {Question, ok, [GenTuple || GenTuple <- ResourcesTail, not kurremkarmerruk_zone:authoritative_for(element(1, GenTuple), Namespace)]},
                            prune_authoritative_from_results(Rest, Namespace, [Tuple1|Acc]);
                        false ->
                            % If we take away the most recent cname, and there's still one
                            % after that, the whole meaning of the answer changes...
                            % Thus we have to split the result
                            Tuple1 = {setelement(1, Question, CanonDomain), TupleState, {SoaRr, []}},
                            prune_authoritative_from_results(Rest, Namespace, [{Question, ok, [GenTuple || GenTuple <- Resources, not kurremkarmerruk_zone:authoritative_for(element(1, GenTuple), Namespace)]}, Tuple1|Acc])
                    end
            end;
        % Referrals
        {_, referral, [{NsRr, _}|_] = Resources} ->
            case kurremkarmerruk_zone:authoritative_for(element(1, NsRr), Namespace) of
                true -> prune_authoritative_from_results(Rest, Namespace, Acc);
                false ->
                    Fn = fun ({FunNsRr, _}) -> {FunNsRr, [GenTuple || GenTuple <- Resources, not kurremkarmerruk_zone:authoritative_for(element(1, GenTuple), Namespace)]} end,
                    Tuple1 = setelement(3, Tuple, lists:map(Fn, Resources)),
                    prune_authoritative_from_results(Rest, Namespace, [Tuple1|Acc])
            end;
        {_, addressless_referral, [NsRr|_]} ->
            case kurremkarmerruk_zone:authoritative_for(element(1, NsRr), Namespace) of
                true -> prune_authoritative_from_results(Rest, Namespace, Acc);
                false -> prune_authoritative_from_results(Rest, Namespace, [Tuple|Acc])
            end;
        {Question, cname, {CnameRr, Resources}} -> % What do?
            case kurremkarmerruk_zone:authoritative_for(element(1, CnameRr), Namespace) of
                true ->
                    Tuple1 = {Question, ok, [GenTuple || GenTuple <- Resources, not kurremkarmerruk_zone:authoritative_for(element(1, GenTuple), Namespace)]},
                    prune_authoritative_from_results(Rest, Namespace, [Tuple1|Acc]);
                false ->
                    Tuple1 = setelement(3, Tuple, {CnameRr, [GenTuple || GenTuple <- Resources, not kurremkarmerruk_zone:authoritative_for(element(1, GenTuple), Namespace)]}),
                    prune_authoritative_from_results(Rest, Namespace, [Tuple1|Acc])
            end;
        {_, cname_loop, Resources} -> % What do?
            Tuple1 = setelement(3, Tuple, [GenTuple || GenTuple <- Resources, not kurremkarmerruk_zone:authoritative_for(element(1, GenTuple), Namespace)]),
            prune_authoritative_from_results(Rest, Namespace, [Tuple1|Acc]);
        {Question, cname_referral, {CnameRr, Referral, Resources}} -> % What do?
            Tuple1 = {Question, cname, {CnameRr, Resources}},
            prune_authoritative_from_results([Referral, Tuple1|Rest], Namespace, Acc);
        _ -> prune_authoritative_from_results(Rest, Namespace, [Tuple|Acc])
    end.


lookup_resource(NamespaceId, Question) ->
    lookup_resource(NamespaceId, Question, erlang:monotonic_time(second), []).

lookup_resource(NamespaceId0, {Domain, Type, Class}=Question, Now, Acc) ->
    NamespaceId1 = namespace:id(NamespaceId0),
    case is_name_error(NamespaceId1, Domain, Class, Now, true) of
        {true, Deadline, Soa} -> {{name_error, setelement(4, Soa, valid_for(Now, Deadline))}, prune_old_entries(Acc)};
        {false, _NsRecord} ->
            Id = namespace:domain_hash(Domain, NamespaceId1),
            case ets:lookup(dns_cache, Id) of
                [] -> prune_old_entries(Acc);
                [{Id, ValidUntil, _}] when Now > ValidUntil -> prune_old_entries(Acc);
                [{Id, _, Map}] ->
                    case maps:get(Class, Map, #{}) of
                        #{Type := {nodata, TypeValidUntil, _}} when Now > TypeValidUntil -> prune_old_entries(Acc);
                        #{Type := {nodata, TypeValidUntil, Soa}} -> {{nodata, setelement(4, Soa, valid_for(Now, TypeValidUntil))}, prune_old_entries(Acc)};
                        #{Type := Entries0} -> prune_old_entries(lists:append(Entries0, Acc));
                        #{cname := [{_, _, _, _, CnameDomain}=Entry]} ->
                            case kurremkarmerruk_utils:cname_resource_loop([Entry|Acc]) of
                                true -> {Question, cname_loop, prune_old_entries([Entry|Acc])};
                                false -> lookup_resource(NamespaceId0, {CnameDomain, Type, Class}, Now, [Entry|Acc])
                            end;
                        #{} -> prune_old_entries(Acc)
                    end
            end
    end.


is_name_error(NamespaceId, Domain, Class, Now, CatchNsRecord) ->
    Id = namespace:domain_hash(Domain, NamespaceId),
    case
        case ets:lookup(dns_cache, Id) of
            [] when Domain =:= [] -> false;
            [] -> {next, CatchNsRecord =:= true};
            [{Id, ValidUntil, _}] when Now > ValidUntil -> {next, CatchNsRecord =:= true};
            [{Id, _, Map}] ->
                case maps:get(Class, Map, undefined) of
                    {name_error, Deadline, Soa} when Now < Deadline -> {true, Deadline, Soa};
                    #{ns := NsRecords} when CatchNsRecord -> {next, NsRecords};
                    _ -> {next, CatchNsRecord =:= true}
                end
        end
    of
        {true, _, _}=Tuple -> Tuple;
        false -> false;
        {next, CaughtNsRecord} ->
            case Domain of
                [] -> {false, CaughtNsRecord};
                [_|Next] -> is_name_error(NamespaceId, Next, Class, Now, CaughtNsRecord)
            end
    end.


cache_namespace(Namespace) ->
    erlang:phash2(Namespace).

%%%%

% Should we store the resource source server?
% What about authoritative flag?
store_resources([], _, _, Trie, _) ->
    Trie;
store_resources([{_, _, _, 0, _}|Entries], NamespaceId, Tab, Trie, MaxTtl) ->
    store_resources(Entries, NamespaceId, Tab, Trie, MaxTtl);
store_resources([{Domain, Type, Class, Ttl0, Data}|Entries], NamespaceId, Tab, Trie, MaxTtl) ->
    Ttl = min(MaxTtl, Ttl0),
    case dnsrr:from_to(Type, atom, module) of
        Type -> store_resources(Entries, NamespaceId, Tab, Trie, MaxTtl); % Don't store stuff we don't understand
        Module ->
            case
                case erlang:function_exported(Module, cacheable, 0) of
                    false -> true;
                    true -> Module:cacheable()
                end
            of
                false -> store_resources(Entries, NamespaceId, Tab, Trie, MaxTtl);
                true ->
                    Id = namespace:domain_hash(Domain, NamespaceId),
                    case ets:lookup(Tab, Id) of
                        [] ->
                            ValidUntil = valid_until(Ttl),
                            Map = #{Class => #{Type => [{Domain, Type, Class, ValidUntil, Data}]}},
                            true = ets:insert(Tab, {Id, ValidUntil, Map}),
                            store_resources(Entries, NamespaceId, Tab, dnstrie:set(namespace:domain_trie_key(Domain, NamespaceId), Id, Trie), MaxTtl);
                        [{Id, ValidUntil, Map0}] when Type =:= cname ->
                            NewValidUntil = valid_until(Ttl),
                            Map = Map0#{Class => #{Type => [{Domain, Type, Class, NewValidUntil, Data}]}},
                            Updates =
                                if
                                    ValidUntil < NewValidUntil -> [{2, NewValidUntil}, {3, Map}];
                                    true -> [{3, Map}]
                                end,
                            true = ets:update_element(Tab, Id, Updates),
                            store_resources(Entries, NamespaceId, Tab, dnstrie:set(namespace:domain_trie_key(Domain, NamespaceId), Id, Trie), MaxTtl);
                        [{Id, ValidUntil, Map0}] ->
                            NewValidUntil = valid_until(Ttl),
                            ClassMap = maps:get(Class, Map0, #{}),
                            TypeRrs0 = maps:get(Type, ClassMap, []),
                            Now = nowts(),
                            TypeRrs = case Type of
                                soa -> [{Domain, Type, Class, NewValidUntil, Data}];
                                _ -> [{Domain, Type, Class, NewValidUntil, Data}|[GenTuple || GenTuple <- TypeRrs0, element(4, GenTuple) > Now]]
                            end,
                            Map = Map0#{Class => ClassMap#{Type => TypeRrs}},
                            Updates =
                                if
                                    ValidUntil < NewValidUntil -> [{2, NewValidUntil}, {3, Map}];
                                    true -> [{3, Map}]
                                end,
                            true = ets:update_element(Tab, Id, Updates),
                            store_resources(Entries, NamespaceId, Tab, Trie, MaxTtl)
                    end
            end
    end.


store_interpret_results([], _, _, Trie, _, _) ->
    Trie;
store_interpret_results([{_, ok, Resources}|Rest], NamespaceId, Tab, Trie, MaxTtl, MaxNegTtl) ->
    store_interpret_results(Rest, NamespaceId, Tab, store_resources(Resources, NamespaceId, Tab, Trie, MaxTtl), MaxTtl, MaxNegTtl);
store_interpret_results([{_, referral, Resources0}|Rest], NamespaceId, Tab, Trie, MaxTtl, MaxNegTtl) ->
    Resources = lists:foldl(fun ({FunNsRr, FunAddressRrs}, Acc) -> lists:append([FunNsRr|FunAddressRrs], Acc) end, [], Resources0),
    store_interpret_results(Rest, NamespaceId, Tab, store_resources(Resources, NamespaceId, Tab, Trie, MaxTtl), MaxTtl, MaxNegTtl);
store_interpret_results([{_, addressless_referral, Resources}|Rest], NamespaceId, Tab, Trie, MaxTtl, MaxNegTtl) ->
    store_interpret_results(Rest, NamespaceId, Tab, store_resources(Resources, NamespaceId, Tab, Trie, MaxTtl), MaxTtl, MaxNegTtl);
store_interpret_results([{_, cname, {_, Resources}}|Rest], NamespaceId, Tab, Trie, MaxTtl, MaxNegTtl) ->
    store_interpret_results(Rest, NamespaceId, Tab, store_resources(Resources, NamespaceId, Tab, Trie, MaxTtl), MaxTtl, MaxNegTtl);
store_interpret_results([{_, AnswerType, {{_, _, _, 0, _}, Resources}}|Rest], NamespaceId, Tab, Trie, MaxTtl, MaxNegTtl)
when AnswerType =:= name_error; AnswerType =:= nodata ->
    store_interpret_results(Rest, NamespaceId, Tab, store_resources(Resources, NamespaceId, Tab, Trie, MaxTtl), MaxTtl, MaxNegTtl);
store_interpret_results([{{Domain0, Type, Class}, AnswerType, {{_, _, _, Ttl0, _}=SoaRr, Resources}}|Rest], NamespaceId, Tab, Trie, MaxTtl, MaxNegTtl)
when AnswerType =:= name_error; AnswerType =:= nodata ->
    Domain = case Resources of
        [] -> Domain0;
        [{_, cname, _, _, CanonDomain}|_] -> CanonDomain
    end,
    Id = namespace:domain_hash(Domain, NamespaceId),
    Ttl = min(MaxNegTtl, Ttl0),
    case ets:lookup(Tab, Id) of
        [] ->
            NewMap = case AnswerType of
                name_error -> #{Class => {name_error, valid_until(Ttl), SoaRr}};
                nodata -> #{Class => #{Type => {nodata, valid_until(Ttl), SoaRr}}}
            end,
            true = ets:insert_new(Tab, {Id, valid_until(Ttl), NewMap});
        [{Id, ValidUntil, Map}] ->
            NewValidUntil = valid_until(Ttl),
            NewMap = case AnswerType of
                name_error -> Map#{Class => {name_error, NewValidUntil, SoaRr}};
                nodata ->
                    ClassMap = maps:get(Class, Map, #{}),
                    Map#{Class => ClassMap#{Type => {nodata, NewValidUntil, SoaRr}}}
            end,
            Updates =
                if
                    ValidUntil < NewValidUntil -> [{2, NewValidUntil}, {3, NewMap}];
                    true -> [{3, NewMap}]
                end,
            true = ets:update_element(Tab, Id, Updates)
    end,
    store_interpret_results(Rest, NamespaceId, Tab, store_resources(Resources, NamespaceId, Tab, Trie, MaxTtl), MaxTtl, MaxNegTtl);
store_interpret_results([Tuple|Rest], NamespaceId, Tab, Trie, MaxTtl, MaxNegTtl) when tuple_size(Tuple) =:= 2 ->
    store_interpret_results(Rest, NamespaceId, Tab, Trie, MaxTtl, MaxNegTtl).


valid_for(Now, Ttl) when Ttl > Now ->
    Ttl - Now;
valid_for(_, _) ->
    0.


valid_until(Ttl) ->
    nowts() + Ttl.


nowts() ->
    erlang:monotonic_time(second).


init_authority_root(State = #server_state{tab=Tab,trie=Trie0}) ->
    case
        case application:get_env(root_server_hints) of
            {ok, {priv_file, App, File}}          -> {path, filename:join(code:priv_dir(App), File), []};
            {ok, {priv_file, App, File, TmpOpts}} -> {path, filename:join(code:priv_dir(App), File), TmpOpts};
            {ok, {file, TmpPath}}                 -> {path, TmpPath, []};
            {ok, {file, TmpPath, TmpOpts}}        -> {path, TmpPath, TmpOpts}
        end
    of
        {path, Path, Opts} ->
            case dnsfile:consult(Path, Opts) of
                {error, Reason} ->
                    error_logger:error_msg("~s~n", [dnserror_format:consult_error(Reason)]),
                    State;
                {ok, Rrs} ->
                    [{_, _, _, MinTtl, _}|_] = lists:sort(fun ({_, _, _, GenTtl1, _}, {_, _, _, GenTtl2, _}) -> GenTtl1 < GenTtl2 end, Rrs),
                    case MinTtl of
                        _ when MinTtl > 1 ->
                            Trie1 = store_resources(Rrs, default, Tab, Trie0, ?MAX_TTL),
                            Timer = timer:send_after(timer:seconds(MinTtl div 2), refresh_authority_cache),
                            State#server_state{trie=Trie1,authority_cache_refresh_timer=Timer}
                    end
            end
    end.


init_namespace_caches([], State) ->
    State;
init_namespace_caches([{_, Netmasks}|Namespaces], State) when is_list(Netmasks) ->
    init_namespace_caches(Namespaces, init_namespace_caches(Netmasks, State));
init_namespace_caches([{_Id0, Settings}|Namespaces], State) ->
    case Settings of
        #{cache := private} ->
            RootFile = filename:join(code:priv_dir(kurremkarmerruk), "root"),
            {ok, _Rrs} = dnsfile:consult(RootFile, [{class, in}]),
            init_namespace_caches(Namespaces, State);
        _ -> init_namespace_caches(Namespaces, State)
    end.


prune_old_entries(Entries) ->
    Now = nowts(),
    [
        {EntryDomain, Type, Class, ValidUntil - Now, Data} ||
        {EntryDomain, Type, Class, ValidUntil, Data} <- Entries, Now =< ValidUntil orelse ValidUntil =:= forever
    ].
