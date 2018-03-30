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
-module(kurremkarmerruk_zone).
-behavior(kurremkarmerruk_handler).
-export([execute/2,valid_opcodes/0,config_keys/0,config_init/1,handle_config/3,config_end/1]).

-export([
    execute_query/2,
    authoritative_for/2,
    namespace_zone_enabled/1,
    override_authoritative/3
]).

valid_opcodes() -> query.
config_keys() -> [zone,refuse_types].

config_init(Map) ->
    Map#{
        zone_config => #{
            tab => rr,
            zones => dnstrie:new(),
            rrs => dnstrie:new()
        }
    }.


config_end(Map = #{zone_config := #{rrs := Trie}}) ->
    case dnstrie:is_empty(Trie) of
        false -> Map;
        true -> maps:remove(zone_config, Map)
    end.


handle_config(zone, ZoneSpec, Namespace = #{zone_config := ZoneConfig, namespace_id := NSID}) ->
    #{
        tab := Tab,
        zones := ZoneTrie0,
        rrs := RRTrie0
    } = ZoneConfig,
    case
        case ZoneSpec of
            {priv_file, App, TmpPath} -> {path, filename:join(code:priv_dir(App), TmpPath), []};
            {priv_file, App, TmpPath, TmpOpts} -> {path, filename:join(code:priv_dir(App), TmpPath), TmpOpts};
            {file, TmpPath} -> {path, TmpPath, []};
            {file, TmpPath, TmpOpts} -> {path, TmpPath, TmpOpts};
            {zone_transfer, TmpApex, TmpMaster} -> {zone_transfer, TmpApex, TmpMaster, [{class, in}]};
            {zone_transfer, TmpApex, TmpMaster, TmpOpts} -> {zone_transfer, TmpApex, TmpMaster, TmpOpts};
            RrList when is_list(RrList) -> {list, RrList}
        end
    of
        {path, Path, ConsultOpts} ->
            case dnsfile:consult(Path, ConsultOpts) of
                {ok, Rrs} ->
                    {ok, ZoneTrie1, RRTrie1} = handle_zone(Rrs, NSID, Tab, ZoneTrie0, RRTrie0),
                    {ok, Namespace#{zone_config => ZoneConfig#{zones := ZoneTrie1, rrs := RRTrie1}}};
                {error, Reason} ->
                    error_logger:error_msg("~s~n", [dnserror_format:consult_error(Reason)]),
                    {error, consult_error}
            end;
        %{zone_transfer, _, _} ->
        Rrs when is_list(Rrs) ->
            % Should process lists and parse any data provided as strings...
            {ok, ZoneTrie1, RRTrie1} = handle_zone(Rrs, NSID, Tab, ZoneTrie0, RRTrie0),
            {ok, Namespace#{zone_config => ZoneConfig#{zones := ZoneTrie1, rrs := RRTrie1}}}
    end.


execute(#{'Questions' := Questions, transport := Transport} = Message, Namespace = #{zone_config := _}) ->
    case is_zone_transfer(Questions) of
        true when Transport =:= udp -> {stop, dnsmsg:set_response_header(Message, return_code, refused)};
        true -> error(implement_zone_transfer); % Only allow zone transfer over tls or tcp, not both?
        false ->
            Answers = execute_query(Questions, Namespace),
            ContainsGlue = [] =/= [GenTuple || GenTuple <- Answers, element(2, GenTuple) =:= referral orelse element(2, GenTuple) =:= addressless_referral],
            {ok, kurremkarmerruk_handler:add_answers(Answers, not ContainsGlue, Message)}
    end;
execute(Message, _) ->
    {ok, Message}.


execute_query(Query, #{namespace_id := NSID, zone_config := ZoneConfig}) when is_list(Query) ->
    #{
        tab := Tab,
        rrs := Trie,
        zones := Zones
    } = ZoneConfig,
    question(Query, NSID, Tab, Trie, Zones, []);
execute_query(Query, Namespace) when is_tuple(Query) ->
    execute_query([Query], Namespace).


namespace_zone_enabled(#{zone_config := _}) ->
    true;
namespace_zone_enabled(_) ->
    false.


override_authoritative([], _, Acc) ->
    Acc;
override_authoritative([Tuple|Rest], Namespace, Acc) when element(2, element(1, Tuple)) =:= cname ->
    override_authoritative(Rest, Namespace, [Tuple|Acc]);
override_authoritative([Tuple|Rest], Namespace, Acc) ->
    case Tuple of
        % What about referral, cname_loop, cname_referral?
        {_, ok, Resources} -> Resources;
        {_, cname, {_, Resources}} -> Resources;
        {_, cname_loop, Resources} -> Resources;
        {_, cname_referral, {_, _, Resources}} -> Resources;
        {_, nodata, {_, Resources}} -> Resources;
        {_, name_error, {_, Resources}} -> Resources;
        _ -> Resources=[]
    end,
    case lists:splitwith(fun ({_, cname, _, _, Domain}) -> not kurremkarmerruk_zone:authoritative_for(Domain, Namespace); (_) -> true end, lists:reverse(Resources)) of
        {_, []} -> override_authoritative(Rest, Namespace, [Tuple|Acc]);
        {Keep0, [CnameRr|_]} -> % This Rr has to follow from a cname.
            {_, Type, Class}=Question=element(1, Tuple),
            % When following cnames, only the domain can change. Thus we can take
            % Domain from the previous cname, Type and Class from the original question
            {_, _, _, _, CanonDomain} = CnameRr,
            Keep1 = [CnameRr|lists:reverse(Keep0)],
            AccTuple = case lists:last(kurremkarmerruk_zone:execute_query({CanonDomain, Type, Class}, Namespace)) of
                {_, ok, ZoneResources} -> {Question, ok, lists:append(ZoneResources, Keep1)};
                {_, cname, {ZoneCnameRr, ZoneResources}} -> {Question, cname, {ZoneCnameRr, lists:append(ZoneResources, Keep1)}};
                {_, cname_loop, ZoneResources} -> {Question, cname_loop, lists:append(ZoneResources, Keep1)};
                {Question, cname_referral, {ZoneCnameRr, Referral, ZoneResources}} ->
                    {Question, cname_referral, {ZoneCnameRr, Referral, lists:append(ZoneResources, Keep1)}};
                {_, AnswerType, {SoaRr, ZoneResources}} when AnswerType =:= nodata; AnswerType =:= name_error ->
                    {Question, AnswerType, {SoaRr, lists:append(ZoneResources, Keep1)}};
                {_, AnswerType, _}=Referral when AnswerType =:= referral; AnswerType =:= addressless_referral ->
                    {Question, cname_referral, {CnameRr, Referral, Keep1}}
            end,
            override_authoritative(Rest, Namespace, [AccTuple|Acc])
    end.


authoritative_for(Domain, #{zone_config := #{zones := Zones}}) ->
    case dnstrie:get_path(namespace:domain_trie_key(Domain), Zones) of
        {none, []} -> false;
        {_, [Res|_]} -> is_tuple(Res)
    end.


is_zone_transfer([]) -> false;
is_zone_transfer([{_, axfr, _}|_]) -> true;
is_zone_transfer([_|Rest]) -> is_zone_transfer(Rest).


set_soa_ttl(Soa) ->
    Min = element(7, element(5, Soa)),
    Ttl = element(4, Soa),
    setelement(4, Soa, min(Min, Ttl)).


wildcard_domain_map_fn(Domain) ->
    fun
        ({'_', _, _, _, _}=Tuple) -> setelement(1, Tuple, Domain);
        (Tuple) -> Tuple
    end.


collect_cnames(Answers) ->
    {Cnames, Other} = lists:partition(fun (FunTuple) -> element(2, FunTuple) =:= cname end, Answers),
    Chains = chain_cnames(Cnames, []),
    terminate_cnames(Chains, Other, []).


chain_cnames([], Acc) ->
    Acc;
chain_cnames([Cname|Rest]=Cnames, Acc) ->
    {{_, _, {CnameRr, _}} = Terminal, Other} = terminal_cname(Cname, Rest, Cnames),
    chain_cnames(CnameRr, Terminal, Other, Acc).


terminal_cname({_, cname, {CnameRr, _}}=Cname, Rest, Cnames) ->
    CnameDomain = dnslib:normalize(element(5, CnameRr)),
    case [GenTuple || GenTuple <- Rest, dnslib:normalize(element(1, element(1, GenTuple))) =:= CnameDomain] of
        [] -> {Cname, lists:filter(fun (FunTuple) -> FunTuple =/= Cname end, Cnames)};
        [New] -> terminal_cname(New, lists:filter(fun (FunTuple) -> FunTuple =/= New end, Cnames), Cnames)
    end.

chain_cnames(CnameRr, {Question, cname, {CnameRr, Resources}}=CnameAnswer, Rest, Acc) ->
    CnameDomain = dnslib:normalize(element(5, CnameRr)),
    case lists:partition(fun (FunTuple) -> dnslib:normalize(element(1, element(1, FunTuple))) =:= CnameDomain end, Rest) of
        {[], _} -> chain_cnames(Rest, [CnameAnswer|Acc]);
        {[{_, cname, {NewCnameRr, NewResources}}], Other} -> chain_cnames(NewCnameRr, {Question, cname, {NewCnameRr, lists:append(NewResources, Resources)}}, Other, Acc)
    end.

terminate_cnames([], Other, Acc) ->
    lists:append(Acc, Other);
terminate_cnames([{Question, cname, {CnameRr, Resources}}=Tuple|Rest], Other, Acc) ->
    CnameDomain = dnslib:normalize(element(5, CnameRr)),
    case lists:partition(fun (FunTuple) -> dnslib:normalize(element(1, element(1, FunTuple))) =:= CnameDomain end, Other) of
        {[], _} -> terminate_cnames(Rest, Other, [Tuple|Acc]);
        {[End], Other1} ->
            case End of
                {_, _} -> terminate_cnames(Rest, Other1, [Tuple|Acc]);
                {_, ok, EndResources} ->
                    AccTuple = {Question, ok, lists:append(EndResources, Resources)},
                    terminate_cnames(Rest, Other1, [AccTuple|Acc]);
                {_, AnswerType, {Soa, EndResources}} when AnswerType =:= name_error; AnswerType =:= nodata ->
                    AccTuple = {Question, AnswerType, {Soa, lists:append(EndResources, Resources)}},
                    terminate_cnames(Rest, Other1, [AccTuple|Acc]);
                {_, AnswerType, _}=Referral when AnswerType =:= referral; AnswerType =:= addressless_referral ->
                    AccTuple = {Question, cname_referral, {CnameRr, Referral, Resources}},
                    terminate_cnames(Rest, Other1, [AccTuple|Acc])
            end
    end.


question([], _, _, _, _, Acc) ->
    collect_cnames(Acc);
question(All = [Question|Rest], NamespaceId, Tab, Trie, Zones, Acc) ->
    Domain = element(1, Question),
    case dnstrie:get_path(namespace:domain_trie_key(Domain), Zones) of
        {_, []} -> question(Rest, NamespaceId, Tab, Trie, Zones, [{Question, refused}|Acc]);
        {_, [ns|_]} -> question_trie(All, ns, NamespaceId, Tab, Trie, Zones, Acc);
        {_, [Soa|_]} ->
            case ets:lookup(Tab, namespace:domain_hash(Domain, NamespaceId)) of
                [] -> question_trie(All, Soa, NamespaceId, Tab, Trie, Zones, Acc);
                [{_, Map}] -> answer_class(Map, All, Soa, NamespaceId, Tab, Trie, Zones, Acc)
            end
    end.


question_trie(All = [{Domain, _, _}=Question|Rest], Soa, NamespaceId, Tab, Trie, Zones, Acc) ->
    case dnstrie:get_path(namespace:domain_trie_key(Domain, NamespaceId), Trie) of
        {full, [Id|_]} ->
            [{_, Map}] = ets:lookup(Tab, Id),
            answer_class(Map, All, Soa, NamespaceId, Tab, Trie, Zones, Acc);
        {nodata, _}  -> question(Rest, NamespaceId, Tab, Trie, Zones, [{Question, nodata, {set_soa_ttl(Soa), []}}|Acc]);
        {partial, [Id|_]} ->
            [{_, Map}] = ets:lookup(Tab, Id),
            partial(Map, All, Soa, NamespaceId, Tab, Trie, Zones, Acc)
    end.


answer_class(Map, All = [{_, _, Class}=Question|Rest], Soa, NamespaceId, Tab, Trie, Zones, Acc) ->
    case maps:get(Class, Map, undefined) of
        undefined -> question(Rest, NamespaceId, Tab, Trie, Zones, [{Question, name_error, {set_soa_ttl(Soa), []}}|Acc]);
        ClassMap -> unalias(ClassMap, All, Soa, NamespaceId, Tab, Trie, Zones, Acc)
    end.


unalias(ClassMap, All = [{_, cname, _}|_], Soa, NamespaceId, Tab, Trie, Zones, Acc) ->
    answer_accumulate(ClassMap, [cname], [], All, Soa, NamespaceId, Tab, Trie, Zones, Acc);
unalias(ClassMap, All = [{Domain, QType, QClass}=Question|Rest], Soa, NamespaceId, Tab, Trie, Zones, Acc) ->
    case ClassMap of
        #{cname := [{_, Type, Class, Ttl, Data}]} ->
            Fn = wildcard_domain_map_fn(Domain),
            Resource = Fn({Domain, Type, Class, Ttl, Data}),
            Tuple = {Question, cname, {Resource, [Resource]}},
            case alias_loop(dnslib:normalize(Data), Acc) of
                true -> question(Rest, NamespaceId, Tab, Trie, Zones, [Tuple|Acc]);
                false -> question([{Data, QType, QClass}|Rest], NamespaceId, Tab, Trie, Zones, [Tuple|Acc])
            end;
        #{} -> answer_check_ns(ClassMap, All, Soa, NamespaceId, Tab, Trie, Zones, Acc)
    end.


answer_check_ns(ClassMap, All, Soa, NamespaceId, Tab, Trie, Zones, Acc) ->
    case ClassMap of
        #{ns := _, soa := _} -> answer_type_aka(ClassMap, All, Soa, NamespaceId, Tab, Trie, Zones, Acc);
        #{ns := Resources} -> referral(Resources, All, Soa, NamespaceId, Tab, Trie, Zones, Acc); % Produce referral
        _ -> answer_type_aka(ClassMap, All, Soa, NamespaceId, Tab, Trie, Zones, Acc)
    end.


answer_type_aka(Map, All = [{_, Type, _}|_], Soa, NamespaceId, Tab, Trie, Zones, Acc) ->
    case
        case dnsrr:from_to(Type, atom, module) of
            Type -> false;
            Mod -> {erlang:function_exported(Mod, aka, 0), Mod}
        end
    of
        {true, Module} -> answer_accumulate(Map, Module:aka(), [], All, Soa, NamespaceId, Tab, Trie, Zones, Acc);
        _ -> answer_accumulate(Map, [Type], [], All, Soa, NamespaceId, Tab, Trie, Zones, Acc)
    end.


answer_accumulate(_, [], ResAcc, [{Domain, _, _}=Question|Rest], Soa, NamespaceId, Tab, Trie, Zones, Acc) ->
    Tuple = case ResAcc of
        [] -> {Question, nodata, {set_soa_ttl(Soa), []}};
        _ ->
            Fn = wildcard_domain_map_fn(Domain),
            {Question, ok, lists:map(Fn, ResAcc)}
    end,
    question(Rest, NamespaceId, Tab, Trie, Zones, [Tuple|Acc]);
answer_accumulate(Map, [Type|Rest], ResAcc, All, Soa, NamespaceId, Tab, Trie, Zones, Acc) ->
    case
        case Type of
            '_' -> maps:fold(fun (Key, Value, FunAcc) when Key =/= 'ZONE', Key =/= 'DOMAIN' -> lists:append(Value, FunAcc); (_, _, FunAcc) -> FunAcc end, [], Map);
            Type -> maps:get(Type, Map, undefined)
        end
    of
        undefined -> answer_accumulate(Map, Rest, ResAcc, All, Soa, NamespaceId, Tab, Trie, Zones, Acc);
        Resources -> answer_accumulate(Map, Rest, lists:append(Resources, ResAcc), All, Soa, NamespaceId, Tab, Trie, Zones, Acc)
    end.


referral(Resources, [{Domain, _, _}=Question|Rest], _, NamespaceId, Tab, Trie, Zones, Acc) ->
    Fn = wildcard_domain_map_fn(Domain),
    NsAddressRrs = collect_referral_rrs(lists:map(Fn, Resources), Fn, NamespaceId, Tab, Trie, Zones, []),
    Tuple = {Question, referral, NsAddressRrs},
    question(Rest, NamespaceId, Tab, Trie, Zones, [Tuple|Acc]).


collect_referral_rrs([], _, _, _, _, _, Acc) ->
    Acc;
collect_referral_rrs([{Domain, _, Class, _, ServerDomain}=NsRr|Rest], Fn, NamespaceId, Tab, Trie, Zones, Acc) ->
    %case dnstrie:get(namespace:domain_trie_key(ServerDomain, NamespaceId), Zones) of
    case dnslib:in_zone(ServerDomain, Domain) of
        true -> % Not authoritative, but we need to provide glue for...
            case dnstrie:get(namespace:domain_trie_key(ServerDomain, NamespaceId), Trie) of
                {ok, Id} ->
                    case ets:lookup(Tab, Id) of
                        [] -> collect_referral_rrs(Rest, Fn, NamespaceId, Tab, Trie, Zones, [{NsRr, []}|Acc]); % Invalid, we should have at least some addresses as glue...
                        [{_, Map}] ->
                            ClassMap = maps:get(Class, Map, #{}),
                            Inet = maps:get(a, ClassMap, []),
                            Inet6 = maps:get(aaaa, ClassMap, []),
                            collect_referral_rrs(Rest, Fn, NamespaceId, Tab, Trie, Zones, [{NsRr, lists:map(Fn, lists:append(Inet, Inet6))}|Acc])
                    end;
                _ ->
                    % Don't pass along bad glue
                    collect_referral_rrs(Rest, Fn, NamespaceId, Tab, Trie, Zones, Acc)
            end;
        false -> collect_referral_rrs(Rest, Fn, NamespaceId, Tab, Trie, Zones, [{NsRr, []}|Acc])
    end.


partial(Map, All = [{_, _, Class}=Question|Rest], Soa, NamespaceId, Tab, Trie, Zones, Acc) ->
    case
        case maps:get(Class, Map, undefined) of
            undefined -> name_error;
            ClassMap ->
                case ClassMap of
                    #{ns := CaseResources} -> {referral, CaseResources};
                    #{} -> name_error
                end
        end
    of
        name_error when Soa =/= ns ->
            Tuple = {Question, name_error, {set_soa_ttl(Soa), []}},
            question(Rest, NamespaceId, Tab, Trie, Zones, [Tuple|Acc]);
        {referral, Resources} -> referral(Resources, All, Soa, NamespaceId, Tab, Trie, Zones, Acc)
    end.


%additionally(Req, Questions, {_, Type, _, _, _}=Tuple) ->
%    Module = dnsrr:from_to(Type, atom, module),
%    case erlang:function_exported(Module, additionally, 1) of
%        false -> {Req, Questions};
%        true -> merge_additionally(Req, Questions, Module:additionally(Tuple))
%    end.


%merge_additionally(Req, Questions, []) ->
%    {Req, Questions};
%merge_additionally(Req0, Questions0, [Tuple|Rest]) when is_tuple(Tuple) ->
%    {Req1, Questions1} = merge_additionally(Req0, Questions0, Tuple),
%    merge_additionally(Req1, Questions1, Rest);
%merge_additionally(Req, Questions, {_, _, _, _, _}=Tuple) ->
%    {dnsmsg:add_response_additional(Req, Tuple), Questions};
%merge_additionally(Req = #{client := {{_, _, _, _, _, _, _, _}, _}}, Questions, {_, a, _}=Tuple) ->
%    {Req, Questions};
%merge_additionally(Req = #{client := {{_, _, _, _}, _}}, Questions, {_, aaaa, _}=Tuple) ->
%    {Req, Questions};
%merge_additionally(Req, Questions, {_, _, _}=Tuple) ->
%    {Req, [{additional, false, Tuple}|Questions]}.


alias_loop(_, []) ->
    false;
alias_loop(Alias, [{_, cname, {{AnswerAlias, _, _, _, _}, _}}|Answers]) ->
    case namespace:domain_hash(AnswerAlias) of
        Alias -> true;
        _ -> alias_loop(Alias, Answers)
    end;
alias_loop(Alias, [_|Answers]) ->
    alias_loop(Alias, Answers).


handle_zone(Rrs0, NamespaceId, Tab, ZoneTrie, Trie0) ->
    case dnszone:to_zone(Rrs0) of
        {ok, Zone} ->
            {ZoneTrie1, Trie1} = handle_zone1(Zone, namespace:id(NamespaceId), Tab, ZoneTrie, Trie0),
            {ok, ZoneTrie1, Trie1};
        {false, Reason} -> {error, Reason}
    end.


handle_zone1(Zone = #{apex := Apex, resources := Rrs, class := Class}, NamespaceId, Tab, ZoneTrie0, Trie0) ->
    [Soa] = dnszone:query({Apex, soa, Class}, Zone),
    ZoneTrie1 = dnstrie:set(namespace:domain_trie_key(Apex), Soa, ZoneTrie0),
    ZoneTrie2 = dnstrie:set(namespace:domain_trie_key(['_'|Apex]), Soa, ZoneTrie1),
    Fn = fun ({FunDomain0, _, _, _, _}, FunTrie) ->
        FunDomain = dnslib:normalize(FunDomain0),
        case dnstrie:get(namespace:domain_trie_key(FunDomain), FunTrie) of
            {ok, false} -> FunTrie;
            {ok, FunSoa} when FunSoa =:= Soa, FunDomain =/= Apex -> dnstrie:set(namespace:domain_trie_key(FunDomain), ns, FunTrie);
            _ -> FunTrie
        end
    end,
    ZoneTrie3 = lists:foldl(Fn, ZoneTrie2, [GenTuple || GenTuple <- Rrs, element(2, GenTuple) =:= ns]),
    {ZoneTrie3, Trie1} = handle_zone_record([Soa], Apex, NamespaceId, Tab, ZoneTrie3, Trie0),
    handle_zone_record(Rrs, Apex, NamespaceId, Tab, ZoneTrie3, Trie1).


handle_zone_record([], _, _, _, ZoneTrie, Trie) ->
    {ZoneTrie, Trie};
handle_zone_record([Entry|Rest], Apex, NamespaceId, Tab, ZoneTrie, Trie) ->
    Domain = element(1, Entry),
    Id = namespace:domain_hash(Domain, NamespaceId),
    case ets:lookup(Tab, Id) of
        [] ->
            resource_record_new(Entry, Apex, NamespaceId, Tab, Trie, Id),
            handle_zone_record(Rest, Apex, NamespaceId, Tab, ZoneTrie, dnstrie:set(namespace:domain_trie_key(Domain, NamespaceId), Id, Trie));
        [{_, Map0}] ->
            Map = resource_record_add(Entry, Map0, Trie),
            true = ets:update_element(Tab, Id, {2, Map}),
            handle_zone_record(Rest, Apex, NamespaceId, Tab, ZoneTrie, dnstrie:set(namespace:domain_trie_key(Domain, NamespaceId), Id, Trie))
    end.


resource_record_new({Domain0, Type, Class, Ttl, Data}, Apex, NamespaceId, Tab, _Trie, Id) ->
    Domain = case dnslib:is_valid_domain(Domain0) of
        {true, false} -> Domain0;
        {true, true} -> '_'
    end,
    ClassMap = #{
        Type => [{Domain, Type, Class, Ttl, Data}],
        'ZONE' => namespace:domain_hash(Apex, NamespaceId)
    },
    true = ets:insert(Tab, {Id, #{Class => ClassMap, 'DOMAIN' => Domain}}).


resource_record_add(Entry0 = {Domain, Type, Class, Ttl, Data}, Map, _Trie) ->
    Entry = case dnslib:is_valid_domain(Domain) of
        {true, false} -> Entry0;
        {true, true} -> {'_', Type, Class, Ttl, Data}
    end,
    ClassMap = maps:get(Class, Map, #{}),
    Current = maps:get(Type, ClassMap, []),
    Map#{Class => ClassMap#{Type => [Entry|Current]}}.
