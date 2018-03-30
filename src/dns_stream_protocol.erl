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
% This file implements the DNS TCP (RFC1034, RFC1035) and TLS protocols.
-module(dns_stream_protocol).

-behavior(ranch_protocol).
-export([start_link/4,init_ranch/3]).

-export([start_link/2,init_resolver/2,connector/5]).

-record(state, {length=-1, pending=#{}, buf= <<>>, address, port, type, cbinfo}).

start_link(Owner, Opts) ->
    Pid = spawn_link(?MODULE, init_resolver, [Owner, Opts]),
    {ok, Pid}.


init_resolver(Owner, #{cbinfo:=Cbinfo={Mod,_,_,_},port:=Port,address:=Address}) ->
    process_flag(trap_exit, true),
    Type = case tuple_size(Address) of
        4 -> inet;
        8 -> inet6
    end,
    % It's probably a bad idea to block send_acks (and thus gen_server) while we are connecting.
    % spawn a process to create the socket.
    Ref = make_ref(),
    {Connector, Mon} = spawn_monitor(?MODULE, connector, [self(), Ref, Mod, [Address, Port, [{active, false},binary]], Type]),
    await_connection(Owner, Connector, Ref, Mon, #state{address=Address, port=Port, cbinfo=Cbinfo}, []).


await_connection(Owner, Connector, Ref, Mon, State = #state{cbinfo={Mod,_,_,_}}, QueuedMessages) ->
    receive
        {'DOWN', Mon, process, Connector, _} -> death_march(Owner, QueuedMessages);
        {connected, Connector, Ref, Socket} ->
            Connector ! {transfer_socket, self(), Ref},
            receive
                {ok, Connector, Ref} ->
                    receive
                        {'DOWN', Mon, process, Connector, normal} ->
                            % SSL socket is a wrapper around the normal socket, inet:setopts will not work.
                            % And gen_tcp does not offer a setopts function.
                            case Mod of
                                ssl -> ok = Mod:setopts(Socket, [{active, true}]);
                                gen_tcp -> ok = inet:setopts(Socket, [{active, true}])
                            end,
                            send_queued(Owner, Socket, State, QueuedMessages)
                    after
                        500 -> death_march(Owner, QueuedMessages)
                    end
            after
                500 -> death_march(Owner, QueuedMessages)
            end;
        {connection_error, Connector, Ref, Reason} ->
            receive
                {'DOWN', Mon, process, Connector, normal} -> death_march(Owner, Reason, QueuedMessages)
            end;
        {send, Owner, OwnerRef, Msg, _} ->
            kurremkarmerruk_recurse:ack_message_send(Owner, OwnerRef),
            await_connection(Owner, Connector, Ref, Mon, State, [Msg|QueuedMessages])
    after
        5000 -> death_march(Owner, QueuedMessages)
    end.


connector(Pid, Ref, Mod, [Address, Port|_] = Args, AfType) ->
    case apply(Mod, connect, Args) of
        {ok, Socket} ->
            Pid ! {connected, self(), Ref, Socket},
            receive
                {transfer_socket, Pid, Ref} ->
                    ok = Mod:controlling_process(Socket, Pid),
                    Pid ! {ok, self(), Ref},
                    exit(normal)
            after
                500 -> error(failed_to_transfer_ownership)
            end;
        {error, econnrefused} ->
            kurremkarmerruk_recurse:report_error(Address, {econnrefused, tcp, Port}),
            Pid ! {connection_error, self(), Ref, connection_refused},
            exit(normal);
        {error, ehostunreach} ->
            kurremkarmerruk_recurse:address_family_unavailable(AfType),
            Pid ! {connection_error, self(), Ref, bad_af},
            exit(normal);
        {error, enetunreach} ->
            kurremkarmerruk_recurse:address_family_unavailable(AfType),
            Pid ! {connection_error, self(), Ref, bad_af},
            exit(normal);
        {error, eafnosupport} ->
            kurremkarmerruk_recurse:address_family_unavailable(AfType),
            Pid ! {connection_error, self(), Ref, bad_af},
            exit(normal)
    end.


death_march(Owner, Reason, []) ->
    death_march(Owner, Reason);
death_march(Owner, Reason, [Msg|Rest]) ->
    kurremkarmerruk_recurse:reroute_message(dnsmsg:id(Msg), Reason),
    death_march(Owner, Reason, Rest).


death_march(Owner, Reason) ->
    receive
        {'EXIT', _, shutdown} ->
            exit(shutdown);
        {send, Owner, Ref, _, _} ->
            kurremkarmerruk_recurse:reroute_message_send(Owner, Ref, Reason),
            death_march(Owner, Reason)
    after
        200 -> exit(normal)
    end.


send_queued(Owner, Socket, State = #state{cbinfo={Mod,_,_,_}}, Queued) ->
    Fn = send_message(Socket, Mod),
    lists:foreach(Fn, lists:reverse(Queued)),
    Now = erlang:monotonic_time(millisecond),
    Pending = maps:from_list([{dnsmsg:id(GenMsg), Now} || GenMsg <- Queued]),
    loop_resolver(Owner, Socket, State#state{pending=Pending}).


loop_resolver(Owner, Socket, State = #state{pending=Pending, cbinfo={Mod,DataTag,ClosedTag,ErrTag}, address=Address}) ->
    receive
        {'EXIT', _, shutdown} ->
            ok = Mod:close(Socket),
            exit(shutdown);
        {send, Owner, Ref, Msg, _} -> % Msg timeout...
            kurremkarmerruk_recurse:ack_message_send(Owner, Ref),
            {ok, Len, Bin} = dnswire:to_binary(Msg),
            ok = Mod:send(Socket, [<<Len:16>>, Bin]),
            Id = dnsmsg:id(Msg),
            loop_resolver(Owner, Socket, State#state{pending=Pending#{Id => erlang:monotonic_time(millisecond)}});
        {DataTag, Socket, Bin} ->
            case data(Owner, State, Bin) of
                {ok, State1} -> loop_resolver(Owner, Socket, State1);
                {error, _} ->
                    Mod:close(Socket),
                    exit(normal)
            end;
        {ClosedTag, Socket} ->
            Mod:close(Socket),
            % Should make sure that we got all expected responses
            % This process should never be restarted as it will not contain
            % the information necessary to execute the tasks entrusted to it...
            #state{address=Address,port=_Port} = State,
            case map_size(Pending) of
                0 -> ok;
                _ -> kurremkarmerruk_recurse:report_error(Address, missing_responses)
            end,
            exit(normal);
        {ErrTag, Socket, _Reason} ->
            Mod:close(Socket),
            % Should make sure that we got all expected responses
            #state{address=Address,port=Port} = State,
            case map_size(Pending) of
                0 -> ok;
                _ -> kurremkarmerruk_recurse:report_error(Address, missing_responses)
            end,
            kurremkarmerruk_recurse:report_error(Address, {transport_error, tcp, Port}),
            exit(normal)
    after
        10000 -> % How long should a socket linger?
            % If we have pending requests, count negatively towards the addresses rep
            Mod:close(Socket),
            case map_size(Pending) of
                0 -> ok;
                _ -> kurremkarmerruk_recurse:report_error(Address, missing_responses)
            end,
            exit(normal)
    end.


data(_, State, <<>>) ->
    {ok, State};
data(Owner, State = #state{length=-1,buf=Buf0,address=Address}, Data) ->
    case <<Buf0/binary, Data/binary>> of
        <<0:16, _/binary>> ->
            kurremkarmerruk_recurse:report_error(Address, unsolicited_message),
            {error, close};
        <<Len:16, Tail/binary>> -> data(Owner, State#state{length=Len}, Tail);
        Buf -> data(Owner, State#state{buf=Buf}, <<>>)
    end;
data(Owner, State = #state{length=Length,buf=Buf,pending=Pending,address=Address}, Data) ->
    case <<Buf/binary, Data/binary>> of
        <<Msg:Length/binary, Tail/binary>> ->
            <<Id:16, _/binary>> = Msg,
            case Pending of
                % Are we expecting the Ids to be in certain order?
                % RFC 1035 says nothing about the order of messages
                #{Id := Timestamp} ->
                    case dnswire:from_binary(<<Buf/binary, Msg/binary>>) of
                        {ok, Result, <<>>} ->
                            kurremkarmerruk_recurse:return_result(Owner, Result),
                            kurremkarmerruk_recurse:update_address_reputation(Address, erlang:monotonic_time(millisecond) - Timestamp, dnsmsg:udp_payload_max_size(Result)),
                            data(Owner, State#state{pending=maps:remove(Id, Pending),buf= <<>>,length=-1}, Tail);
                        _ ->
                            kurremkarmerruk_recurse:report_error(Address, format_error),
                            {error, close}
                    end;
                #{} ->
                    kurremkarmerruk_recurse:report_error(Address, unsolicited_message),
                    {error, close}
            end;
        NewBuf -> State#state{buf=NewBuf}
    end.


start_link(Ref, _, Transport, [Namespaces]) ->
    Pid = spawn_link(?MODULE, init_ranch, [Ref, Transport, Namespaces]),
    {ok, Pid}.


% Absolute limit on messages/lifetime for a client connection?
init_ranch(Ref, Transport, Namespaces) ->
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
                client => Peer,
                timestamp => erlang:monotonic_time(millisecond),
                transport => transport_to_atom(Transport)
            },
            case kurremkarmerruk_handler:execute_handlers(Msg1, Namespace) of
                drop -> drop;
                {ok, Responses} when is_list(Responses) -> lists:foreach(Send, Responses);
                {ok, Response} when is_map(Response) -> Send(Response)
            end;
        {ok, Req, _} -> Send(dnsmsg:response(Req, #{return_code => format_error})); % Message with trailing bytes
        {error, {format_error, _, Msg}} -> Send(dnsmsg:response(Msg, #{return_code => format_error}))
    end.


send_message(Socket, Transport) ->
    fun (Msg) ->
        {ok, Len, Bin} = dnswire:to_iolist(Msg),
        ok = Transport:send(Socket, [<<Len:16>>,Bin])
    end.


transport_to_atom(ranch_tcp) -> tcp;
transport_to_atom(ranch_ssl) -> tls.
