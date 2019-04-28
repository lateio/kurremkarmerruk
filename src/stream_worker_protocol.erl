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
% This file implements the DNS TCP (RFC1034, RFC1035) and TLS protocols.
-module(stream_worker_protocol).

-behavior(ranch_protocol).
-export([start_link/4,init/3]).

%-record(state, {length=-1, pending=#{}, buf= <<>>, address, port, type, cbinfo}).


start_link(Ref, _, Transport, [Namespaces]) ->
    Pid = spawn_link(?MODULE, init, [Ref, Transport, Namespaces]),
    {ok, Pid}.


% Absolute limit on messages/lifetime for a client connection?
init(Ref, Transport, Namespaces) ->
    {ok, Socket} = ranch:handshake(Ref),
    {ok, {Address, _}} = Transport:peername(Socket),
    Namespace = namespace:match(Address, Namespaces),
    loop_ranch(Socket, Transport, init, <<>>, Namespace).


loop_ranch(Socket, Transport, State, _, Namespace)
when State =:= init; State =:= message_length ->
    Timeout = case State of
        init -> 1000;
        _ -> 5000
    end,
    MaxLen = case Namespace of
        undefined -> 512;
        #{client_msg_size_drop := CaseLen} -> CaseLen
    end,
    case Transport:recv(Socket, 2, Timeout) of
        timeout -> Transport:close(Socket);
        {ok, <<Len:16>>} when Len > MaxLen ->
            Transport:close(Socket),
            exit(normal);
        {ok, <<Len:16>>} -> loop_ranch(Socket, Transport, Len, <<>>, Namespace);
        {error, closed} -> ok;
        {error, _Reason} -> error
    end;
loop_ranch(Socket, Transport, Len, Buf, Namespace) ->
    Left = Len - byte_size(Buf),
    case Transport:recv(Socket, Left, 2000) of
        timeout -> Transport:close(Socket);
        {error, closed} -> ok;
        {error, _Reason} -> error;
        {ok, Data} when byte_size(Data) =:= Left ->
            handle_message(Socket, Transport, <<Buf/binary, Data/binary>>, Namespace),
            loop_ranch(Socket, Transport, message_length, <<>>, Namespace)
    end.


handle_message(Socket, Transport, Bin, Namespace) ->
    MaxLen = case Namespace of
        undefined -> 512;
        #{client_msg_size_refuse := CaseMaxLen} -> CaseMaxLen
    end,
    Send = send_message(Socket, Transport),
    case dnswire:from_binary(Bin) of
        {ok, Req, <<>>} when byte_size(Bin) > MaxLen ->
            Res = dnsmsg:response(Req, #{return_code => refused}),
            Send(Res#{'Questions' := []});
        {ok, Msg0, <<>>} ->
            {ok, Peer} = Transport:peername(Socket),
            Msg1 = Msg0#{
                peer => Peer,
                timestamp => erlang:monotonic_time(millisecond),
                transport => transport_to_atom(Transport)
            },
            case kurremkarmerruk_handler:execute_handlers(Msg1, Namespace) of
                drop -> drop;
                {ok, Responses} when is_list(Responses) -> lists:foreach(Send, Responses);
                {ok, Response} when is_map(Response) -> Send(Response);
                {passthrough, List} -> lists:foreach(Send, List)
            end;
        {ok, Req, _} -> Send(dnsmsg:response(Req, #{return_code => format_error})); % Message with trailing bytes
        {error, _, ErrMsg} ->
            Res = dnsmsg:response(ErrMsg),
            Send(Res)
        %_ -> Need to close the connection...
    end.


send_message(Socket, Transport) ->
    fun
        ({Len, Bin}) -> ok = Transport:send(Socket, [<<Len:16>>,Bin]);
        (Msg) ->
            {ok, Len, Bin} = dnswire:to_iolist(Msg),
            ok = Transport:send(Socket, [<<Len:16>>,Bin])
    end.


transport_to_atom(ranch_tcp) -> tcp;
transport_to_atom(ranch_ssl) -> tls.
