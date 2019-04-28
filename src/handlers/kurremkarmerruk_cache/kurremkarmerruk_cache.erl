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
-module(kurremkarmerruk_cache).
-behavior(kurremkarmerruk_handler).
-export([execute/2,valid_opcodes/0,config_keys/0,config_init/1,handle_config/3,config_end/1,spawn_handler_proc/0]).

-behavior(gen_server).
-export([init/1,code_change/3,terminate/2,handle_call/3,handle_cast/2,handle_info/2]).

-export([
    namespace_cache_enabled/1,
    start_link/0,
    store_resources/2,
    %store_negative/2,
    store_interpret_results/2,
    lookup_resource/1,
    lookup_resource/2,
    delete_cache/0,
    %lookup_ns/2,
    cache_namespace/1,
    cache_invalidator/3,
    execute_query/2
    %keep_cached/2
]).

-include_lib("dnslib/include/dnslib.hrl").

spawn_handler_proc() -> false.
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
        cache_id => default,
        cache_unknown_type => false,
        cache_unknown_class => false
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


execute(Msg = #{questions := Questions}, Namespace = #{cache_config := _}) ->
% Don't consider cache if recursion is not desired?
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
check_cache([Entry|Questions], CacheId, Namespace, ZoneAvailable, Acc0) ->
    % Check if Entry type is actually cacheable
    % What if Entry type all/maila? Or class is any?
    Acc1 = case lookup_resource(Entry, [{namespace, Namespace}]) of
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
                        [{_, cname_referral, {ZoneCnameRr, ZoneReferral, ZoneResources}}] -> {Entry, cname_referral, {ZoneCnameRr, ZoneReferral, lists:append(ZoneResources, CacheResources)}};
                        [{_, referral, ZoneReferrals}] -> {Entry, referral, ZoneReferrals};
                        [{_, addressless_referral, ZoneReferrals}] -> {Entry, addressless_referral, ZoneReferrals}
                        % What about just plain referrals?
                    end;
                Res -> Res
            end,
            check_cache(Questions, CacheId, Namespace, ZoneAvailable, [Tuple|Acc0])
    end,
    check_cache(Questions, CacheId, Namespace, ZoneAvailable, Acc1).


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

-record(server_state, {
    tab,
    trie=dnstrie:new(),
    invalidator,
    invalidator_ref,
    invalidator_mon
}).


init([]) ->
    process_flag(trap_exit, true),
    Tab = ets:new(dns_cache, [named_table, ordered_set]),
    timer:send_interval(timer:minutes(1), invalidate_cache),
    State = #server_state{tab=Tab}, % init_authority_root(#server_state{tab=Tab}),
    {ok, State}.


code_change(_OldVsn, _State, _Extra) ->
    {ok, _State}.


terminate(_Reason, _State) ->
    ok.


handle_call({store, Namespace, Entries}, _, State = #server_state{tab=Tab, trie=Trie}) ->
    CacheConfig = maps:get(cache_config, Namespace),
    Ttl = maps:get(max_ttl, CacheConfig, 86400),
    CacheId = maps:get(cache_id, CacheConfig),
    {reply, ok, State#server_state{trie=store_resources(Entries, CacheId, Tab, Trie, Ttl)}};
handle_call({store_interpret_result, Namespace, Results}, _, State = #server_state{tab=Tab, trie=Trie}) ->
    CacheConfig = maps:get(cache_config, Namespace),
    {Ttl, TtlNeg} = {maps:get(max_ttl, CacheConfig, 86400), maps:get(max_negative_ttl, Namespace, 86400)},
    CacheId = maps:get(cache_id, CacheConfig),
    {reply, ok, State#server_state{trie=store_interpret_results(Results, CacheId, Tab, Trie, Ttl, TtlNeg)}};
handle_call(_Msg, _From, _State) ->
    {noreply, _State}.


handle_cast({delete_cache, '_'}, State = #server_state{tab=Tab}) ->
    true = ets:delete_all_objects(Tab),
    {noreply, State};
handle_cast(_Msg, _State) ->
    {noreply, _State}.


handle_info(invalidate_cache, State = #server_state{tab=Tab,invalidator_ref=undefined,invalidator=undefined}) ->
    Ref = make_ref(),
    {Invalidator, Mon} = spawn_monitor(?MODULE, cache_invalidator, [self(), Ref, Tab]),
    {noreply, State#server_state{invalidator_ref=Ref,invalidator=Invalidator,invalidator_mon=Mon}};
handle_info({cache_invalidator_res, Ref, Drop, Check}, State = #server_state{tab=Tab,invalidator_ref=Ref}) ->
    [ets:delete(Tab, GenKey) || GenKey <- Drop],
    cache_invalidate_check(Check, Tab, nowts()),
    {noreply, State#server_state{invalidator=undefined}};
handle_info({'DOWN', Mon, process, Invalidator, _}, State = #server_state{invalidator=Invalidator,invalidator_mon=Mon}) ->
    {noreply, State#server_state{invalidator_mon=undefined,invalidator=undefined}};
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


store_resources(Namespace = #{cache_config := #{cache_unknown_type := UnknownType, cache_unknown_class := UnknownClass}}, Resources0) when is_list(Resources0) ->
    Resources1 = case kurremkarmerruk_zone:namespace_zone_enabled(Namespace) of
        true -> [GenTuple || GenTuple <- Resources0, not kurremkarmerruk_zone:authoritative_for(element(1, GenTuple), Namespace)];
        false -> Resources0
    end,
    Resources2 = case {UnknownType, UnknownClass} of
        {true, true} -> Resources1;
        {false, true} -> [GenTuple || GenTuple <- Resources1, is_atom(element(2, GenTuple))];
        {true, false} -> [GenTuple || GenTuple <- Resources1, is_atom(element(3, GenTuple))];
        {false, false} -> [GenTuple || GenTuple <- Resources1, is_atom(element(2, GenTuple)), is_atom(element(3, GenTuple))]
    end,
    % How to avoid storing the same resource multiple times...
    gen_server:call(?MODULE, {store, Namespace, Resources2});
%store_resources(NamespaceId, Resources) when is_list(Resources), is_atom(NamespaceId) ->
%    gen_server:call(?MODULE, {store, NamespaceId, Resources});
store_resources(Namespace, Resource) when not is_list(Resource) ->
    store_resources(Namespace, [Resource]).


store_interpret_results(Namespace = #{cache_config := #{cache_unknown_type := UnknownType, cache_unknown_class := UnknownClass}}, Results0) when is_list(Results0) ->
    Results1 = case kurremkarmerruk_zone:namespace_zone_enabled(Namespace) of
        true -> prune_authoritative_from_results(Results0, Namespace, []);
        false -> Results0
    end,
    Results2 = case {UnknownType, UnknownClass} of
        {true, true} -> Results1;
        _ -> prune_unknown_from_results(Results1, UnknownType, UnknownClass, [])
    end,
    gen_server:call(?MODULE, {store_interpret_result, Namespace, Results2});
%store_interpret_results(NamespaceId, Results) when is_list(Results), is_atom(NamespaceId) ->
%    gen_server:call(?MODULE, {store_interpret_result, NamespaceId, Results});
store_interpret_results(Namespace, Result) when not is_list(Result) ->
    store_interpret_results(Namespace, [Result]).


prune_authoritative_from_results([], _, Acc) ->
    Acc;
prune_authoritative_from_results([Tuple|Rest], Namespace, Acc) ->
    % This is awful. Should come up with a consistent way of handling cases like this.
    % But is pruning authoritative needed anywhere else?
    case Tuple of
        {_, ok, Resources} -> prune_authoritative_from_results(Rest, Namespace, [setelement(3, Tuple, [GenTuple || GenTuple <- Resources, not kurremkarmerruk_zone:authoritative_for(GenTuple, Namespace)])|Acc]);
        {Question, TupleState, {SoaRr, Resources}} when TupleState =:= nodata; TupleState =:= name_error ->
            case Resources of
                [] ->
                    case kurremkarmerruk_zone:authoritative_for(Question, Namespace) of
                        true -> prune_authoritative_from_results(Rest, Namespace, Acc);
                        false -> prune_authoritative_from_results(Rest, Namespace, [Tuple|Acc])
                    end;
                [{_, cname, _, _, CanonDomain}=CnameRr|ResourcesTail] ->
                    case kurremkarmerruk_zone:authoritative_for(setelement(1, CnameRr, CanonDomain), Namespace) of
                        true -> % Make answer into an ok, drop canon and prune others
                            Tuple1 = {Question, ok, [GenTuple || GenTuple <- ResourcesTail, not kurremkarmerruk_zone:authoritative_for(GenTuple, Namespace)]},
                            prune_authoritative_from_results(Rest, Namespace, [Tuple1|Acc]);
                        false ->
                            % If we take away the most recent cname, and there's still one
                            % after that, the whole meaning of the answer changes...
                            % Thus we have to split the result
                            Tuple1 = {setelement(1, Question, CanonDomain), TupleState, {SoaRr, []}},
                            prune_authoritative_from_results(Rest, Namespace, [{Question, ok, [GenTuple || GenTuple <- Resources, not kurremkarmerruk_zone:authoritative_for(GenTuple, Namespace)]}, Tuple1|Acc])
                    end
            end;
        % Referrals
        {_, referral, [{NsRr, _}|_] = Resources} ->
            case kurremkarmerruk_zone:authoritative_for(NsRr, Namespace) of
                true -> prune_authoritative_from_results(Rest, Namespace, Acc);
                false ->
                    Fn = fun ({FunNsRr, FunNsResources}) -> {FunNsRr, [GenTuple || GenTuple <- FunNsResources, not kurremkarmerruk_zone:authoritative_for(GenTuple, Namespace)]} end,
                    Tuple1 = setelement(3, Tuple, lists:map(Fn, Resources)),
                    prune_authoritative_from_results(Rest, Namespace, [Tuple1|Acc])
            end;
        {_, addressless_referral, [NsRr|_]} ->
            case kurremkarmerruk_zone:authoritative_for(NsRr, Namespace) of
                true -> prune_authoritative_from_results(Rest, Namespace, Acc);
                false -> prune_authoritative_from_results(Rest, Namespace, [Tuple|Acc])
            end;
        {Question, cname, {CnameRr, Resources}} -> % What do?
            case kurremkarmerruk_zone:authoritative_for(CnameRr, Namespace) of
                true ->
                    Tuple1 = {Question, ok, [GenTuple || GenTuple <- Resources, not kurremkarmerruk_zone:authoritative_for(GenTuple, Namespace)]},
                    prune_authoritative_from_results(Rest, Namespace, [Tuple1|Acc]);
                false ->
                    Tuple1 = setelement(3, Tuple, {CnameRr, [GenTuple || GenTuple <- Resources, not kurremkarmerruk_zone:authoritative_for(GenTuple, Namespace)]}),
                    prune_authoritative_from_results(Rest, Namespace, [Tuple1|Acc])
            end;
        {_, cname_loop, Resources} -> % What do?
            Tuple1 = setelement(3, Tuple, [GenTuple || GenTuple <- Resources, not kurremkarmerruk_zone:authoritative_for(GenTuple, Namespace)]),
            prune_authoritative_from_results(Rest, Namespace, [Tuple1|Acc]);
        {Question, cname_referral, {CnameRr, Referral, Resources}} -> % What do?
            Tuple1 = {Question, cname, {CnameRr, Resources}},
            prune_authoritative_from_results([Referral, Tuple1|Rest], Namespace, Acc);
        {Question, _Error}=Tuple ->
            case kurremkarmerruk_zone:authoritative_for(Question, Namespace) of
                true -> prune_authoritative_from_results(Rest, Namespace, Acc);
                false -> prune_authoritative_from_results(Rest, Namespace, [Tuple|Acc])
            end
        % What is the last case pattern for?
        %_ -> prune_authoritative_from_results(Rest, Namespace, [Tuple|Acc])
    end.


prune_unknown_from_results([], _, _, Acc) ->
    Acc;
prune_unknown_from_results([Result|Rest], UnknownType, false, Acc) ->
    % We can straight up drop answers based on question class
    Question = element(1, Result),
    case is_atom(element(3, Question)) of
        true when UnknownType -> prune_unknown_from_results(Rest, UnknownType, false, [Result|Acc]);
        true when not UnknownType -> prune_unknown_from_results(Rest, UnknownType, false, prune_unknown_from_results([Result], UnknownType, true, Acc));
        false -> prune_unknown_from_results(Rest, UnknownType, false, Acc)
    end;
prune_unknown_from_results([Result|Rest], false, true, Acc) ->
    % Unknown types can really only present in ok and nodata...
    case Result of
        {_, ok, Resources} -> prune_unknown_from_results(Rest, false, true, [setelement(3, Result, [GenTuple || GenTuple <- Resources, is_atom(element(2, GenTuple))])|Acc]);
        {{_, Type, _}=Question, nodata, {_, Resources}} when is_integer(Type) -> prune_unknown_from_results(Rest, false, true, [{Question, ok, Resources}|Acc]);
        _ -> prune_unknown_from_results(Rest, false, true, [Result|Acc])
    end.


lookup_resource(Question) ->
    lookup_resource(Question, [{namespace, namespace:internal()}]).

lookup_resource(Question, Opts) ->
    lookup_resource(Question, Opts, erlang:monotonic_time(second), []).

lookup_resource({Domain, Type, Class}=Question, Opts, Now, Acc) ->
    #{cache_config := #{cache_id := CacheId}} = proplists:get_value(namespace, Opts),
    case is_name_error(CacheId, Domain, Class, Now, true) of
        {true, Deadline, Soa} -> {{name_error, setelement(4, Soa, valid_for(Now, Deadline))}, prune_old_entries(Acc)};
        %{false, _NsRecord} ->
        _FalseRes ->
            Id = namespace:domain_hash(Domain, CacheId),
            case ets:lookup(dns_cache, Id) of
                [] -> prune_old_entries(Acc);
                [{Id, ValidUntil, _}] when Now > ValidUntil -> prune_old_entries(Acc);
                [{Id, _, Map}] ->
                    case maps:get(Class, Map, #{}) of
                        #{Type := {_ErrorType, TypeValidUntil, _}} when Now > TypeValidUntil -> prune_old_entries(Acc);
                        #{Type := {ErrorType, TypeValidUntil, Soa}} -> {{ErrorType, setelement(4, Soa, valid_for(Now, TypeValidUntil))}, prune_old_entries(Acc)};
                        #{Type := Entries0} -> prune_old_entries(lists:append(Entries0, Acc));
                        #{cname := [{_, _, _, _, CnameDomain}=Entry]} ->
                            case kurremkarmerruk_utils:cname_resource_loop([Entry|Acc]) of
                                true -> {Question, cname_loop, prune_old_entries([Entry|Acc])};
                                false -> lookup_resource({CnameDomain, Type, Class}, Opts, Now, [Entry|Acc])
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


delete_cache() ->
    gen_server:cast(?MODULE, {delete_cache, '_'}).


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
                                _ ->
                                    case [Data] -- [?RESOURCE_DATA(GenTuple) || GenTuple <- TypeRrs0] of
                                        [] -> TypeRrs0;
                                        _ -> [{Domain, Type, Class, NewValidUntil, Data}|[GenTuple || GenTuple <- TypeRrs0, element(4, GenTuple) > Now]]
                                    end
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


prune_old_entries(Entries) ->
    Now = nowts(),
    [
        {EntryDomain, Type, Class, ValidUntil - Now, Data} ||
        {EntryDomain, Type, Class, ValidUntil, Data} <- Entries, Now =< ValidUntil
    ].
