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
-module(client_udp).

-export([start_link/2, init/2, loop/3]).

-record(state, {pending=#{}, timeout=timer:seconds(20), type}).


start_link(Owner, Opts) ->
    Pid = spawn_link(?MODULE, init, [Owner, Opts]),
    {ok, Pid}.


init(Owner, #{socket_type := Type}) ->
    % Do we need the Type parameter?
    process_flag(trap_exit, true),
    case gen_udp:open(0, [{active, true}, binary, Type]) of
        {ok, Socket} -> loop(Owner, Socket, #state{type=Type});
        {error, eafnosupport} ->
            kurremkarmerruk_recurse:address_family_unavailable(Type),
            death_march(Owner)
    end.


loop(Owner, Socket, State = #state{timeout=Timeout, pending=Pending, type=Type}) ->
    receive
        {'EXIT', _, shutdown} ->
            ok = gen_udp:close(Socket),
            exit(shutdown);
        {send, Owner, Ref, {{Address, Port}, Msg}, _} ->
            kurremkarmerruk_recurse:ack_message_send(Owner, Ref),
            {ok, Len, Bin} = dnswire:to_iolist(Msg),
            if
                % In the future we need to consider server advertised udp payload size, instead of static 512 bytes
                Len > 512 ->
                    kurremkarmerruk_recurse:message_requires_tcp(dnsmsg:id(Msg)),
                    loop(Owner, Socket, State);
                true ->
                    case gen_udp:send(Socket, Address, Port, Bin) of
                        ok -> loop(Owner, Socket, State#state{pending=Pending#{{dnsmsg:id(Msg), Address, Port} => erlang:monotonic_time(millisecond)}});
                        {error, ehostunreach} ->
                            kurremkarmerruk_recurse:address_family_unavailable(Type),
                            kurremkarmerruk_recurse:reroute_message(Msg, bad_af),
                            death_march(Owner);
                        {error, enetunreach} ->
                            kurremkarmerruk_recurse:address_family_unavailable(Type),
                            kurremkarmerruk_recurse:reroute_message(Msg, bad_af),
                            death_march(Owner)
                    end
            end;
        {udp, Socket, Address, Port, Bin = <<Id:16, _/binary>>} ->
            Key = {Id, Address, Port},
            case Pending of
                % Do we require that the id should match the address and port?
                #{Key := Sent} ->
                    case dnswire:from_binary(Bin) of
                        {ok, Msg = #{'Is_response' := true}, <<>>} ->
                            RequestTime = erlang:monotonic_time(millisecond) - Sent,
                            kurremkarmerruk_recurse:return_result(Owner, Msg#{latency => RequestTime}),
                            kurremkarmerruk_recurse:update_address_reputation(Address, RequestTime, dnsmsg:udp_payload_max_size(Msg)),
                            loop(Owner, Socket, State#state{pending=maps:remove(Key, Pending)});
                        _ ->
                            kurremkarmerruk_recurse:report_error(Address, format_error),
                            kurremkarmerruk_recurse:reroute_message(Id, format_error),
                            loop(Owner, Socket, State)
                    end;
                #{} ->
                    kurremkarmerruk_recurse:report_error(Address, unsolicited_message),
                    loop(Owner, Socket, State)
            end
    after
        Timeout ->
            case map_size(Pending) of
                0 ->
                    gen_udp:close(Socket),
                    exit(normal);
                _ ->
                    % There's probably no need to signal recurse about these messages,
                    % as they'll probably have timeouted already
                    %[kurremkarmerruk_recurse() || {Id, _, _} <- maps:keys(Pending)]
                    gen_udp:close(Socket),
                    exit(normal)
            end
    end.


death_march(Owner) ->
    receive
        {'EXIT', _, shutdown} ->
            exit(shutdown);
        {send, Owner, Ref, _, _} ->
            kurremkarmerruk_recurse:reroute_message_send(Owner, Ref),
            death_march(Owner)
    after
        200 -> exit(normal)
    end.
