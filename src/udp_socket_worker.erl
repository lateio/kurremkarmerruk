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
-module(udp_socket_worker).

-export([start_link/3, init/3, worker/4, loop/2]).

start_link(Namespaces, Fd, Af) ->
    % Should we actually pass namespaces here?
    % After we'll allow changes to namespaces after the
    % initial setup in the future, all workers will need
    % to refresh namespace config before they do anything with it...
    Pid = spawn_link(?MODULE, init, [Namespaces, Fd, Af]),
    {ok, Pid}.


init(Namespaces, Fd, inet) ->
    {ok, Socket} = gen_udp:open(0, [{fd, socketfd:get(Fd)},{active, false},binary,inet]),
    loop(Namespaces, Socket);
init(Namespaces, Fd, inet6) ->
    {ok, Socket} = gen_udp:open(0, [{fd, socketfd:get(Fd)},{active, false},binary,inet6]),
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
            ok = gen_udp:send(Socket, Address, Port, ResBin);
        {ok, Req0, <<>>} ->
            Req = Req0#{
                client => {Address, Port},
                timestamp => erlang:monotonic_time(millisecond),
                transport => udp
            },
            MaxResLen = maps:get('EDNS_udp_payload_size', Req0, 512),
            case kurremkarmerruk_handler:execute_handlers(Req, Namespace) of
                drop -> ok;
                {ok, Response} ->
                    {ok, Len, ResBin} = dnswire:to_iolist(Response),
                    % Change to respect edns payload suggestion
                    case Len of
                        _ when Len > MaxResLen ->
                            % When we have to truncate, just drop all payload because the client will try
                            % again anyway. We can serve them from the cache
                            Truncres0 = Response#{'Answers' => [], 'Nameservers' => [], 'Additional' => []},
                            Truncres = dnsmsg:header(Truncres0, truncated, true),
                            {ok, _, TruncBin} = dnswire:to_binary(Truncres),
                            ok = gen_udp:send(Socket, Address, Port, TruncBin);
                        _ -> ok = gen_udp:send(Socket, Address, Port, ResBin)
                    end
            end;
        {ok, Req, _} -> % Respond with format_error to messages with trailing bytes
            Res = dnsmsg:response(Req, #{return_code => format_error}),
            {ok, _, ResBin} = dnswire:to_iolist(Res),
            ok = gen_udp:send(Socket, Address, Port, ResBin);
        {error, {format_error, _, Req}} ->
            Res = dnsmsg:response(Req, #{return_code => format_error}),
            {ok, _, ResBin} = dnswire:to_iolist(Res),
            ok = gen_udp:send(Socket, Address, Port, ResBin);
        _ -> ok
    end.
