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
-module(udp_socket_worker).

-export([start_link/1, init/3, worker/4, loop/2]).

start_link(Fd) ->
    % Should we actually pass namespaces here?
    % After we'll allow changes to namespaces after the
    % initial setup in the future, all workers will need
    % to refresh namespace config before they do anything with it...
    Namespaces = kurremkarmerruk_server:namespaces(),
    Pid = spawn_link(?MODULE, init, [Namespaces, Fd, socketfd:address_family(Fd)]),
    {ok, Pid}.


init(_, _, any) ->
    error(any);
init(Namespaces, Fd, Af) ->
    {ok, Socket} = gen_udp:open(0, [{fd, socketfd:get(Fd)}, {active, false}, binary, Af]),
    loop(Namespaces, Socket).


loop(Namespaces, Socket) ->
    {ok, Tuple={Address, _, _}} = gen_udp:recv(Socket, 1500),
    add_length_limit(namespace:match(Address, Namespaces), Socket, Tuple),
    ?MODULE:loop(Namespaces, Socket).


add_length_limit(_, _, {{255,255,255,255}, _, _}) ->
    % Is this even possible?
    skip;
add_length_limit(undefined, Socket, Tuple) ->
    worker(undefined, Socket, Tuple, {512, 512});
add_length_limit(Namespace, Socket, Tuple) when is_map(Namespace) ->
    worker(Namespace, Socket, Tuple, {maps:get(client_msg_size_refuse, Namespace), maps:get(client_msg_size_drop, Namespace)}).


worker(_, _, {_, _, Data}, {_, DropLen}) when byte_size(Data) > DropLen ->
    drop_msg;
worker(Namespace, Socket, {Address, Port, Data}, {RefuseLen, _}) ->
    % Have a worker pool, throw message to them
    %pool_worker_server:message(Address, Port, Data, Socket)
    case dnswire:from_binary(Data) of
        {ok, Req, <<>>} when byte_size(Data) > RefuseLen ->
            Res = dnsmsg:response(Req, #{return_code => refused}),
            {ok, _, ResBin} = dnswire:to_iolist(Res#{'Questions' := []}),
            % Need to handle send errors
            % ehostdown
            % eaddrnotavail... What do?
            ok = gen_udp:send(Socket, Address, Port, ResBin);
        {ok, Req0, <<>>} ->
            Req = Req0#{
                peer => {Address, Port},
                timestamp => erlang:monotonic_time(millisecond),
                transport => udp
            },
            MaxResLen = maps:get('EDNS_udp_payload_size', Req0, 512),
            case kurremkarmerruk_handler:execute_handlers(Req, Namespace) of
                drop -> ok;
                {ok, Response} ->
                    case dnswire:to_iolist(Response, [{max_length, MaxResLen}]) of
                        {ok, _, ResBin} -> ok = gen_udp:send(Socket, Address, Port, ResBin);
                        {partial, _, ResBin, _} -> ok = gen_udp:send(Socket, Address, Port, ResBin)
                    end
            end;
        {ok, Req, _} -> % Respond with format_error to messages with trailing bytes
            Res = dnsmsg:response(Req, #{return_code => format_error}),
            {ok, _, ResBin} = dnswire:to_iolist(Res),
            ok = gen_udp:send(Socket, Address, Port, ResBin);
        {error, _, ErrMsg} ->
            Res = dnsmsg:response(ErrMsg),
            {ok, _, ResBin} = dnswire:to_iolist(Res),
            ok = gen_udp:send(Socket, Address, Port, ResBin);
        _ -> ok
    end.
