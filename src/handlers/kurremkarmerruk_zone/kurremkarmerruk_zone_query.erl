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
-module(kurremkarmerruk_zone_query).

-export([
    execute/2,
    execute_query/2
]).

-include_lib("dnslib/include/dnslib.hrl").


execute(#{questions := Questions} = Message, Namespace) ->
    {Authoritative, Answers} = execute_query_internal(Questions, Namespace),
    {ok, kurremkarmerruk_handler:add_answers(Answers, Authoritative, Message)};
execute(Message, _) ->
    {ok, Message}.


execute_query(Query, Namespace) when is_list(Query) ->
    case execute_query_internal(Query, Namespace) of
        {_, Answers} -> Answers
    end;
execute_query(Query, Namespace) when is_tuple(Query) ->
    execute_query([Query], Namespace).


execute_query_internal(Query, #{namespace_id := NSID, zone_config := ZoneConfig}) when is_list(Query) ->
    #{
        module := Module,
        fulfill_queries := AllowQuery
    } = ZoneConfig,
    if
        AllowQuery -> question(Query, NSID, Module, true, []);
        not AllowQuery -> {false, [{GenQuestion, refused} || GenQuestion <- Query]}
    end;
execute_query_internal(Query, Namespace) when is_tuple(Query) ->
    execute_query_internal([Query], Namespace).


set_soa_ttl(Soa) ->
    Min = element(7, element(5, Soa)),
    Ttl = element(4, Soa),
    setelement(4, Soa, min(Min, Ttl)).


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
    CnameDomain = dnslib:normalize_domain(element(5, CnameRr)),
    case [GenTuple || GenTuple <- Rest, dnslib:normalize_domain(element(1, element(1, GenTuple))) =:= CnameDomain] of
        [] -> {Cname, lists:filter(fun (FunTuple) -> FunTuple =/= Cname end, Cnames)};
        [New] -> terminal_cname(New, lists:filter(fun (FunTuple) -> FunTuple =/= New end, Cnames), Cnames)
    end.

chain_cnames(CnameRr, {Question, cname, {CnameRr, Resources}}=CnameAnswer, Rest, Acc) ->
    CnameDomain = dnslib:normalize_domain(element(5, CnameRr)),
    case lists:partition(fun (FunTuple) -> dnslib:normalize_domain(element(1, element(1, FunTuple))) =:= CnameDomain end, Rest) of
        {[], _} -> chain_cnames(Rest, [CnameAnswer|Acc]);
        {[{_, cname, {NewCnameRr, NewResources}}], Other} -> chain_cnames(NewCnameRr, {Question, cname, {NewCnameRr, lists:append(NewResources, Resources)}}, Other, Acc)
    end.

terminate_cnames([], Other, Acc) ->
    lists:append(Acc, Other);
terminate_cnames([{Question, cname, {CnameRr, Resources}}=Tuple|Rest], Other, Acc) ->
    CnameDomain = dnslib:normalize_domain(element(5, CnameRr)),
    case lists:partition(fun (FunTuple) -> dnslib:normalize_domain(element(1, element(1, FunTuple))) =:= CnameDomain end, Other) of
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


question([], _, _, Authoritative, Acc) ->
    {Authoritative, collect_cnames(Acc)};
question([Question|Rest], NamespaceId, Module, WasAuthoritative, Acc) ->
    %Domain = element(1, Question),
    % Need to check aka...
    % Need handle authoritative/non
    case kurremkarmerruk_zone_storage:get_resource(Question, Module, NamespaceId) of
        {ok, IsAuthoritative, Resources} -> question(Rest, NamespaceId, Module, WasAuthoritative andalso IsAuthoritative, [{Question, ok, Resources}|Acc]);
        {cname, Resource} ->
            Tuple = {Question, cname, {Resource, [Resource]}},
            case alias_loop(dnslib:normalize_domain(element(5, Resource)), Acc) of
                true -> question(Rest, NamespaceId, Module, WasAuthoritative, [setelement(2, Tuple, cname_loop)|Acc]);
                false -> question([{element(5, Resource), element(2, Question), element(3, Resource)}|Rest], NamespaceId, Module, WasAuthoritative, [Tuple|Acc])
            end;
        {name_error, Soa} -> question(Rest, NamespaceId, Module, WasAuthoritative, [{Question, name_error, {set_soa_ttl(Soa), []}}|Acc]);
        {nodata, Soa} -> question(Rest, NamespaceId, Module, WasAuthoritative, [{Question, nodata, {set_soa_ttl(Soa), []}}|Acc]);
        {referral, Resources} -> referral(Resources, Question, Rest, NamespaceId, Module, WasAuthoritative andalso false, Acc);
        not_in_zone -> question(Rest, NamespaceId, Module, WasAuthoritative andalso false, [{Question, refused}|Acc])
    end.


referral(Resources, Question, Rest, NamespaceId, Module, false, Acc) ->
    NsAddressRrs = collect_referral_rrs(Resources, NamespaceId, Module, []),
    Tuple = {Question, referral, NsAddressRrs},
    question(Rest, NamespaceId, Module, false, [Tuple|Acc]).


collect_referral_rrs([], _, _, Acc) ->
    lists:reverse(Acc);
collect_referral_rrs([{_, _, Class, _, ServerDomain}=NsRr|Rest], NamespaceId, Module, Acc) ->
    ARes = case kurremkarmerruk_zone_storage:get_resource({ServerDomain, a, Class}, Module, NamespaceId) of
        {ok, false, ARrs} -> ARrs;
        _ -> []
    end,
    AAAARes = case kurremkarmerruk_zone_storage:get_resource({ServerDomain, aaaa, Class}, Module, NamespaceId) of
        {ok, false, AAAARrs} -> AAAARrs;
        _ -> []
    end,
    collect_referral_rrs(Rest, NamespaceId, Module, [{NsRr, lists:append(ARes, AAAARes)}|Acc]).


%additionally(Req, Questions, Resource) ->
%   Additionally = dnsrr:additionally(Resource),
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


-spec alias_loop(integer(), [dnsmsg:interpret_result()]) -> boolean().
alias_loop(_, []) ->
    false;
alias_loop(Alias, [{_, cname, {{AnswerAlias, _, _, _, _}, _}}|Answers]) ->
    case namespace:domain_hash(AnswerAlias) of
        Alias -> true;
        _ -> alias_loop(Alias, Answers)
    end;
alias_loop(Alias, [_|Answers]) ->
    alias_loop(Alias, Answers).
