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
-module(kurremkarmerruk_zone_transfer).

-export([
    execute/2
]).

-include_lib("dnslib/include/dnslib.hrl").


execute(#{'Questions' := [{Apex, _, Class} = Question], transport := Transport} = Message, Namespace = #{zone_config := #{allow_zone_transfer := Zones}})
when Transport =/= udp ->
    case allow_zone_transfer(dnslib:normalize_question(Question), Message, Zones) of
        false -> {stop, dnsmsg:set_response_header(Message, return_code, refused)};
        _ -> % Always return an axfr, if transfer is allowed...
            #{
                namespace_id := NSID,
                zone_config := #{ module := Module }
            } = Namespace,
            {ok, Zone} = kurremkarmerruk_zone_storage:get_zone(Apex, Class, Module, NSID),
            {[Soa], Resources} = lists:partition(fun (Tuple) -> element(2, Tuple) =:= soa end, Zone),
            Message1 = dnsmsg:set_response_section(Message, answer, lists:append([Soa|Resources], [Soa])),
            Passthrough = to_transfer_messages(dnsmsg:response(Message1), []),
            {passthrough, Passthrough}
    end;
execute(Message, _) ->
    {stop, dnsmsg:set_response_header(Message, return_code, refused)}.


allow_zone_transfer(_, _, '_') -> true;
allow_zone_transfer(_, _, []) -> false;
allow_zone_transfer({Apex, Type, Class}, _, [{Apex, Class}|_]) ->
    if
        Type =:= axfr -> axfr;
        Type =:= ixfr -> ixfr
    end;
allow_zone_transfer({Apex, _, Class}, #{transport := Transport, peer := {Address, _}}, [{Apex, Class, [_|_] = OptList}|_]) ->
    case lists:member(tls, OptList) of
        true when Transport =/= tls -> false;
        _ ->
            % Restrict transfer based on ip...
            case proplists:get_value(ip, OptList) of
                undefined -> true;
                List ->
                    Fn = fun (Netmask) -> not namespace:match_netmask(Address, Netmask) end,
                    case lists:splitwith(Fn, List) of
                        {_, []} -> false;
                        _ -> true
                    end
            end
    end;
%allow_zone_transfer({Apex, Type, Class}=Question, [{Apex, Class, }|Rest]) ->
    % Allow transfer only from NS addresses...
    %allow_zone_transfer(Question, Rest);
allow_zone_transfer(Question, Message, [_|Rest]) ->
    allow_zone_transfer(Question, Message, Rest).


% It's a bad idea to create messages in memory and then send them all of them at once...
% How to allow message -> send, message -> send immediately as the messages are created?
% Allow some type of stream handling?
% Or allow the zone_storage modules to optionally implement "zone streaming"...
% Then we'll handle it as we can...
to_transfer_messages(Message, Acc) ->
    case dnswire:to_iolist(Message, [{max_length, 16#3FF}, {truncate, false}]) of
        {ok, Len, Io} -> lists:reverse([{Len, Io}|Acc]);
        {partial, Len, Io, {Q, An, Au, Ad}} ->
            to_transfer_messages(
                dnsmsg:set_section(
                    dnsmsg:set_section(
                        dnsmsg:set_section(
                            dnsmsg:set_section(Message, additional, Ad),
                        authority, Au),
                    answer, An),
                question, Q),
            [{Len, Io}|Acc])
    end.
