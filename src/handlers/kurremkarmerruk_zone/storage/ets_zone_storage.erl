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
-module(ets_zone_storage).

-behavior(kurremkarmerruk_zone_storage).
-export([
    init/2,
    add_zone/4,
    add_resource/3,
    get_resource/3,
    get_zone/4,
    authoritative_for/4,
    delete_zone/4
]).

-ifdef(EUNIT).
-include_lib("eunit/include/eunit.hrl").
-endif.

authority_key(NsId) -> erlang:phash2([NsId, authority]).

zone_key(NsId, Soa) -> erlang:phash2([NsId, dnslib:normalize_domain(element(1, Soa)), element(3, Soa)]).

complete_key(NsId, Soa) -> erlang:phash2([NsId, dnslib:normalize_domain(element(1, Soa)), element(3, Soa), complete]).

init(StorageName, _) ->
    Tab = ets:new(StorageName, [{read_concurrency, true}, named_table, public]),
    Tab.


add_zone(Tab, Soa, Edges0, NsID) ->
    Key = authority_key(NsID),
    Zones0 = case ets:lookup(Tab, Key) of
        [] -> dnstrie:new();
        [{Key, Trie}] -> Trie
    end,
    ApexDomain0 = element(1, Soa),
    Class = element(3, Soa),
    ApexDomain1 = dnslib:normalize_domain(ApexDomain0),
    ApexDomain2 = namespace:domain_trie_key(ApexDomain1, [Class]),
    Zones1 = case dnstrie:get(ApexDomain2, Zones0) of
        {ok, ns} -> dnstrie:set(ApexDomain2, Soa);
        _ -> dnstrie:set(ApexDomain2, Soa, Zones0)
    end,
    Fn = fun ({FunNsDomain, FunType, FunClass, _, _}, FunTrie) ->
        FunDomain = namespace:domain_trie_key(FunNsDomain, [FunClass]),
        case dnstrie:get(FunDomain, FunTrie) of
            {ok, {ns, _}} -> FunTrie;
            {ok, FunTuple} when tuple_size(FunTuple) =:= 5 -> FunTrie;
            _ -> dnstrie:set(FunDomain, {ns, namespace:domain_hash(FunNsDomain, [NsID, FunClass, FunType])}, FunTrie)
        end
    end,
    Edges = [GenEdge || GenEdge <- Edges0, dnslib:normalize_domain(element(1,GenEdge)) =/= ApexDomain1],
    Zones2 = lists:foldl(Fn, Zones1, Edges),
    true = ets:insert(Tab, {Key, Zones2}),
    true = ets:insert_new(Tab, [
        {zone_key(NsID, Soa), dnstrie:new()},
        {complete_key(NsID, Soa), []}
    ]),
    ok.


add_resource(Tab, Resource, NsId) when not is_list(Resource) ->
    add_resource(Tab, [Resource], NsId);
add_resource(Tab, Resources, NsId) ->
    case ets:lookup(Tab, authority_key(NsId)) of
        [] -> error(no_zones);
        [{_, AuthorityTrie}] ->
            First = hd(Resources),
            case get_soa(element(1, First), element(3, First), AuthorityTrie) of
                not_in_zone -> not_in_zone;
                {Soa, _} ->
                    [{_, CompleteResources}] = ets:lookup(Tab, complete_key(NsId, Soa)),
                    [{_, ZoneTrie}] = ets:lookup(Tab, zone_key(NsId, Soa)),
                    add_resource_internal(Tab, Resources, NsId, AuthorityTrie, dnslib:normalize_resource(Soa), ZoneTrie, lists:reverse(CompleteResources), #{})
            end
    end.

add_resource_internal(Tab, [], NsId, _AuthorityTrie, Soa, ZoneTrie, Complete, Entries) ->
    true = ets:insert(Tab, [
        {zone_key(NsId, Soa), ZoneTrie},
        {complete_key(NsId, Soa), lists:reverse(Complete)}
        |maps:values(Entries)
    ]),
    ok;
add_resource_internal(Tab, [Resource0|Rest], NsId, AuthorityTrie, Soa, ZoneTrie0, CompleteResources, Entries0) ->
    Domain = element(1, Resource0),
    Class = element(3, Resource0),
    Type = element(2, Resource0),
    case dnslib:domain_in_zone(dnslib:normalize_domain(Domain), element(1, Soa)) of
        false -> error(not_in_zone);
        true ->
            Resource = case Domain of
                ['_'|_] -> setelement(1, Resource0, '_');
                _ -> Resource0
            end,
            {_, [PathTuple|_]} = dnstrie:get_path(namespace:domain_trie_key(element(1, Resource0), [Class]), AuthorityTrie),
            Authoritative = case tuple_size(PathTuple) of
                2 -> false;
                5 -> true
            end,
            Id = namespace:domain_hash(Domain, [NsId, Class, Type]),
            Entries1 = case maps:get(Id, Entries0, undefined) of
                undefined ->
                    case ets:lookup(Tab, Id) of
                        [] -> Entries0#{Id => {Id, Authoritative, [Resource]}};
                        [{Id, RRList}] ->
                            % Need to check that all entries match the domain
                            % Unlikely or not, phash2 will at some point produce collisions
                            Entries0#{Id => {Id, Authoritative, lists:append(RRList, [Resource])}}
                    end;
                EntryTuple -> Entries0#{Id => setelement(3, EntryTuple, lists:append(element(3, EntryTuple), [Resource]))}
            end,
            ZoneTrieKey = namespace:domain_trie_key(Domain, [Class]),
            ZoneTrie1 = case Domain of
                ['_'|_] ->
                    case dnstrie:get(ZoneTrieKey, ZoneTrie0, false) of
                        {ok, #{Type := _}} -> ZoneTrie0;
                        {ok, Map} -> dnstrie:set(ZoneTrieKey, Map#{Type => Id}, ZoneTrie0);
                        _ -> dnstrie:set(ZoneTrieKey, #{Type => Id}, ZoneTrie0)
                    end;
                _ ->
                    case dnstrie:get(ZoneTrieKey, ZoneTrie0, false) of
                        {ok, List} when Type =/= cname -> dnstrie:set(ZoneTrieKey, lists:usort([Type|List]), ZoneTrie0);
                        _ when Type =:= cname -> dnstrie:set(ZoneTrieKey, cname, ZoneTrie0);
                        _ -> dnstrie:set(ZoneTrieKey, [Type], ZoneTrie0)
                    end
            end,
            add_resource_internal(Tab, Rest, NsId, AuthorityTrie, Soa, ZoneTrie1, [Resource0|CompleteResources], Entries1)
    end.


get_soa(Domain, Class, AuthorityTrie) ->
    {_, Res} = dnstrie:get_path(namespace:domain_trie_key(Domain, [Class]), AuthorityTrie),
    get_soa(Res, true).

get_soa([], _) ->
    not_in_zone;
get_soa([{ns, _}|Rest], _) ->
    get_soa(Rest, false);
get_soa([Soa = {_, _, _, _, _}|_], IsAuthoritative) ->
    {Soa, IsAuthoritative}.


get_resource(Tab, {Domain, Type, Class}, NsId) ->
    Key = namespace:domain_hash(Domain, [NsId, Class, Type]),
    case ets:lookup(Tab, Key) of
        [{Key, Authoritative, Resources}] -> {ok, Authoritative, Resources};
        _ -> % check zones
            TrieKey = authority_key(NsId),
            case ets:lookup(Tab, TrieKey) of
                [] -> not_in_zone;
                [{TrieKey, AuthorityTrie}] ->
                    DomainTrieKey = namespace:domain_trie_key(Domain, [Class]),
                    {_, Result} = dnstrie:get_path(DomainTrieKey, AuthorityTrie),
                    case get_resource_result(Result) of
                        {referral, Id} ->
                            [{Id, _, Resources}] = ets:lookup(Tab, Id),
                            {referral, Resources};
                        {soa, Soa} ->
                            [{_, ZoneTrie}] = ets:lookup(Tab, zone_key(NsId, Soa)),
                            case dnstrie:get(DomainTrieKey, ZoneTrie) of
                                {ok, #{Type := Id}} ->
                                    [{Id, _, Resources}] = ets:lookup(Tab, Id),
                                    {ok, true, [setelement(1, GenResource, Domain) || GenResource <- Resources]};
                                {ok, #{cname := Id}} ->
                                    [{Id, _, [Resource]}] = ets:lookup(Tab, Id),
                                    {cname, Resource};
                                {ok, #{}} -> {nodata, Soa};
                                {ok, cname} ->
                                    [{_, _, [Resource]}] = ets:lookup(Tab, namespace:domain_hash(Domain, [NsId, Class, cname])),
                                    {cname, Resource};
                                {ok, _} -> {nodata, Soa};
                                nodata -> {nodata, Soa};
                                undefined -> {name_error, Soa}
                            end;
                        undefined -> not_in_zone
                    end
            end
    end.

get_resource_result([{ns, Id}|_]) ->
    {referral, Id};
get_resource_result([Soa = {_, _, _, _, _}|_]) ->
    {soa, Soa};
get_resource_result([]) ->
    undefined.


get_zone(Tab, Apex, Class, NSID) ->
    case ets:lookup(Tab, authority_key(NSID)) of
        [] -> not_authoritative;
        [{_, AuthorityTrie}] ->
            case dnstrie:get(namespace:domain_trie_key(Apex, [Class]), AuthorityTrie) of
                {ok, Soa = {_, _, _, _, _}} ->
                    [{_, Resources}] = ets:lookup(Tab, complete_key(NSID, Soa)),
                    {ok, Resources};
                _ -> not_authoritative
            end
    end.


delete_zone(Tab, Apex, Class, NSID) ->
    case ets:lookup(Tab, authority_key(NSID)) of
        [] -> ok;
        [{_, AuthorityTrie0}] ->
            DomainKey = namespace:domain_trie_key(Apex, [Class]),
            case dnstrie:get(DomainKey, AuthorityTrie0) of
                {ok, Soa = {_, _, _, _, _}} ->
                    [{_, Resources}] = ets:lookup(Tab, complete_key(NSID, Soa)),
                    % Find edges, modify authority_trie
                    Apex = dnslib:normalize_domain(element(1, Soa)),
                    Edges = [GenEdge || GenEdge <- Resources, dnslib:normalize_domain(element(1, GenEdge)) =/= Apex],
                    AuthorityTrie1 = dnstrie:remove(DomainKey, AuthorityTrie0),
                    AuthorityTrie2 = lists:foldl(
                        fun ({FunDomain, _, FunClass, _, _}, FunTrie) ->
                            dnstrie:remove(namespace:domain_trie_key(FunDomain, [FunClass]), FunTrie)
                        end,
                        AuthorityTrie1,
                        Edges),
                    true = ets:insert(Tab, {authority_key(NSID), AuthorityTrie2}),
                    % Delete resources
                    lists:foreach(fun
                        ({Domain = ['_'|_], _, FunClass, _, _}) ->
                            Id = namespace:domain_hash(Domain, [NSID, FunClass]),
                            true = ets:delete(Tab, Id);
                        ({Domain, Type, FunClass, _, _}) ->
                            Id = namespace:domain_hash(Domain, [NSID, FunClass, Type]),
                            true = ets:delete(Tab, Id)
                        end,
                        Resources),
                    % Delete complete zone and trie
                    true = ets:delete(Tab, complete_key(NSID, Soa)),
                    true = ets:delete(Tab, zone_key(NSID, Soa)),
                    ok;
                _ -> ok
            end
    end.


% Pass a copy of that resource?
% Pass a question for that resource? What if we hit a wildcard?
%delete_resource(Tab, )


% On changes the serial should change...
% How to distinguish between changes which alter the serial and those that do not?


authoritative_for(Tab, Domain, Class, NSID) ->
    case ets:lookup(Tab, authority_key(NSID)) of
        [] -> false;
        [{_, Trie}] ->
            case dnstrie:get_path(namespace:domain_trie_key(Domain, [Class]), Trie) of
                {_, [{_, _, _, _, _}|_]} -> true;
                _ -> false
            end
    end.



-ifdef(EUNIT).
% Need to include Authoritative ...
try_all_test() ->
    Tab = init(rr, nil),
    Soa = dnslib:resource("foo IN 60 SOA ns1.foo hostmaster.foo 0 1h 1h 1h 1h"),
    Edges = [
        dnslib:resource("domain.foo IN 60 NS ns1.domain.foo"),
        dnslib:resource("foo IN 60 NS ns1.foo")
    ],
    add_zone(Tab, Soa, Edges, 0),
    Resources = [
        dnslib:resource("ns1.domain.foo IN 60 A 10.10.10.10"),
        dnslib:resource("ns1.foo IN 60 A 11.11.11.11"),
        dnslib:resource("bar.foo IN 60 TXT Hello bar"),
        dnslib:resource("alias.foo IN 60 CNAME bar.foo")
    ],
    Wildcard = dnslib:resource("*.bar.foo IN 60 TXT Hello wildcard"),
    WildcardCNAME = dnslib:resource("*.xyz.bar.foo IN 60 CNAME abc.bar.foo"),
    Zone = [Soa|lists:append(Edges, [Wildcard, WildcardCNAME|Resources])],
    add_resource(Tab, Zone, 0),
    true = authoritative_for(Tab, dnslib:domain("foo"), in, 0),
    {ok, true, [Soa]} = get_resource(Tab, dnslib:question("foo", soa, in), 0),
    AResource = hd(Resources),
    {ok, false, [AResource]} = get_resource(Tab, dnslib:question("ns1.domain.foo", a, in), 0),
    {nodata, Soa} = get_resource(Tab, dnslib:question("bar.foo", a, in), 0),
    {name_error, Soa} = get_resource(Tab, dnslib:question("no-domain.foo", a, in), 0),
    NsResource = hd(Edges),
    {referral, [NsResource]} = get_resource(Tab, dnslib:question("hello.domain.foo", a, in), 0),
    not_in_zone = get_resource(Tab, dnslib:question("", ns, in), 0),
    WildcardMatch = setelement(1, Wildcard, [<<"abc">>,<<"bar">>,<<"foo">>]),
    {ok, true, [WildcardMatch]} = get_resource(Tab, dnslib:question("abc.bar.foo", txt, in), 0),
    {cname, _} = get_resource(Tab, dnslib:question("alias.foo", txt, in), 0),
    {cname, _} = get_resource(Tab, dnslib:question("alias.xyz.bar.foo", txt, in), 0),
    {ok, Zone} = get_zone(Tab, [<<"foo">>], in, 0),
    not_authoritative = get_zone(Tab, [], in, 0),
    ok = delete_zone(Tab, [<<"foo">>], in, 0),
    not_in_zone = get_resource(Tab, dnslib:question("foo", soa, in), 0).
-endif.
