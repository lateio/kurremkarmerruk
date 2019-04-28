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
-module(kurremkarmerruk_utils).
-export([
    parse_netmask/1,
    parse_host_port/2,
    limit_answer_ttls/4,
    cname_resource_loop/1,
    write_terms/2
]).


-spec parse_netmask(string()) ->
    {'ok', {inet:ip4_address(), 0..32}}  |
    {'ok', {inet:ip6_address(), 0..128}} |
    'error'.
parse_netmask(Netmask) ->
    [AddressStr|Tail] = case string:split(Netmask, "/") of
        [Tmp, LengthStr] -> [Tmp, list_to_integer(LengthStr)];
        SplitResult -> SplitResult
    end,
    case
        case inet:parse_ipv4strict_address(AddressStr) of
            {ok, _} = Tuple -> Tuple;
            _ -> inet:parse_ipv6strict_address(AddressStr)
        end
    of
        {ok, {_, _, _, _} = Address} ->
            case Tail of
                [] -> {ok, {Address, 32}};
                [Length] when Length >= 0, Length =< 32 -> {ok, {Address, Length}};
                _ -> error
            end;
        {ok, {_, _, _, _, _, _, _, _} = Address} ->
            case Tail of
                [] -> {ok, {Address, 128}};
                [Length] when Length >= 0, Length =< 128 -> {ok, {Address, Length}};
                _ -> error
            end;
        _ -> error
    end.


-spec parse_host_port(string(), inet:port_number() | 'default')
    -> {'ok', {inet:ip_address(), inet:port_number() | 'default'}} | 'error'.
parse_host_port([$[|Rest], _) ->
    [IPStr, [$:|PortStr]] = string:split(Rest, "]", trailing),
    {ok, Address} = inet:parse_ipv6strict_address(IPStr),
    case list_to_integer(PortStr) of
        Port when Port > 0, Port < 16#FFFF -> {ok, {Address, Port}}
    end;
parse_host_port(Str, DefaultPort) ->
    case inet:parse_ipv6strict_address(Str) of
        {ok, Address} -> {ok, {Address, DefaultPort}};
        _ ->
            case string:split(Str, ":", trailing) of
                [IPStr, PortStr] ->
                    Port = list_to_integer(PortStr),
                    case inet:parse_ipv4strict_address(IPStr) of
                        {ok, Address} when Port > 0, Port < 16#FFFF -> {ok, {Address, Port}};
                        _ when Port > 0, Port < 16#FFFF ->
                            case dnslib:list_to_domain(IPStr) of
                                {ok, _, [Head|_] = Domain} when Domain =/= [], Head =/= '_' ->
                                    true = dnslib:is_valid_hostname(Domain),
                                    {ok, {Domain, Port}}
                            end
                    end;
                [Str] ->
                    case inet:parse_ipv4strict_address(Str) of
                        {ok, Address} -> {ok, {Address, DefaultPort}};
                        _ ->
                            case dnslib:list_to_domain(Str) of
                                {ok, _, [Head|_] = Domain} when Domain =/= [], Head =/= '_' ->
                                    true = dnslib:is_valid_hostname(Domain),
                                    {ok, {Domain, DefaultPort}}
                            end
                    end
            end
    end.


limit_answer_ttls([], _, _, Acc) ->
    Acc;
limit_answer_ttls([{_, AnswerType, Resources0}=Tuple|Rest], MaxTtl, MaxNegTtl, Acc)
when AnswerType =:= ok; AnswerType =:= addressless_referral; AnswerType =:= cname_loop ->
    Resources = [setelement(4, GenTuple, min(GenTtl, MaxTtl)) || {_, _, _, GenTtl, _}=GenTuple <- Resources0],
    limit_answer_ttls(Rest, MaxTtl, MaxNegTtl, [setelement(3, Tuple, Resources)|Acc]);
limit_answer_ttls([{_, cname, {{_, _, _, CnameTtl, _}=CnameRr, Resources0}}=Tuple|Rest], MaxTtl, MaxNegTtl, Acc) ->
    Resources = [setelement(4, GenTuple, min(GenTtl, MaxTtl)) || {_, _, _, GenTtl, _}=GenTuple <- Resources0],
    limit_answer_ttls(Rest, MaxTtl, MaxNegTtl, [setelement(3, Tuple, {setelement(4, CnameRr, min(CnameTtl, MaxTtl)), Resources})|Acc]);
limit_answer_ttls([{_, referral, Resources0}=Tuple|Rest], MaxTtl, MaxNegTtl, Acc) ->
    Resources = [{setelement(4, GenTuple, min(GenTtl, MaxTtl)), [setelement(4, GenGenTuple, min(GenGenTtl, MaxTtl)) || {_, _, _, GenGenTtl}=GenGenTuple <- GenAddressRrs]} || {{_, _, _, GenTtl, _}=GenTuple, GenAddressRrs} <- Resources0],
    limit_answer_ttls(Rest, MaxTtl, MaxNegTtl, [setelement(3, Tuple, Resources)|Acc]);
limit_answer_ttls([{_, AnswerType, {{_, _, _, SoaTtl, _}=Soa, Resources0}}=Tuple|Rest], MaxTtl, MaxNegTtl, Acc)
when AnswerType =:= name_error; AnswerType =:= nodata ->
    Resources = [setelement(4, GenTuple, min(GenTtl, MaxTtl)) || {_, _, _, GenTtl, _}=GenTuple <- Resources0],
    limit_answer_ttls(Rest, MaxTtl, MaxNegTtl, [setelement(3, Tuple, {setelement(4, Soa, min(MaxNegTtl, SoaTtl)), Resources})|Acc]);
limit_answer_ttls([Tuple|Rest], MaxTtl, MaxNegTtl, Acc) when tuple_size(Tuple) =:= 2 ->
    limit_answer_ttls(Rest, MaxTtl, MaxNegTtl, [Tuple|Acc]).


cname_resource_loop([]) -> false;
cname_resource_loop([{_, cname, _, _, _}=Cname|Rest]) ->
    Domain = dnslib:normalize_domain(element(5, Cname)),
    case [GenTuple || GenTuple <- Rest, dnslib:normalize_domain(element(1, GenTuple)) =:= Domain] of
        [] -> cname_resource_loop(Rest);
        _ -> true
    end;
cname_resource_loop([_|Rest]) ->
    cname_resource_loop(Rest).


write_terms(Filename, List) ->
    Format = fun(Term) -> io_lib:format("~tp.~n", [Term]) end,
    Text = lists:map(Format, List),
    ok = file:write_file(Filename, Text),
    ok.
