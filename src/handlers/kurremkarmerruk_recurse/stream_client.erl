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
-module(stream_client).

-export([start_link/2,init/2,connector/5]).

-record(state, {length, pending=#{}, buf= <<>>, address, port, type, cbinfo}).

-import(kurremkarmerruk_recurse_address_reputation, [
    address_family_unavailable/1,
    update_address_reputation/3,
    report_error/2
]).

start_link(Owner, Opts) ->
    Pid = spawn_link(?MODULE, init, [Owner, Opts]),
    {ok, Pid}.


init(Owner, Opts = #{cbinfo:=Cbinfo={Mod,_,_,_},port:=Port,address:=Address}) ->
    process_flag(trap_exit, true),
    Type = case tuple_size(Address) of
        4 -> inet;
        8 -> inet6
    end,
    % It's probably a bad idea to block send_acks (and thus gen_server) while we are connecting.
    % spawn a process to create the socket.
    Timeout = maps:get(timeout, Opts, timer:seconds(5)),
    Ref = make_ref(),
    {Connector, Mon} = spawn_monitor(?MODULE, connector, [self(), Ref, Mod, [Address, Port, [{active, false}, binary], Timeout], Type]),
    await_connection(Owner, Connector, Ref, Mon, #state{address=Address, port=Port, cbinfo=Cbinfo}, []).


await_connection(Owner, Connector, Ref, Mon, State = #state{cbinfo={Mod,_,_,_}}, QueuedMessages) ->
    receive
        {'EXIT', _, shutdown} ->
            exit(Connector, kill),
            exit(normal);
        {'DOWN', Mon, process, Connector, timeout} ->
            death_march(Owner, QueuedMessages);
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
        {connection_error, Connector, Ref, Reason} -> death_march(Owner, Reason, QueuedMessages);
        {send, Pid, Ref, _, <<Id:16, _/binary>> = Bin} when byte_size(Bin) =< 16#FFFF ->
            Pid ! {send_ack, self(), Ref, Id},
            await_connection(Owner, Connector, Ref, Mon, State, [{Pid, Bin}|QueuedMessages])
    end.


connector(Pid, Ref, Mod, [Address, Port|_] = Args, AfType) ->
    case apply(Mod, connect, Args) of
        {ok, Socket} ->
            Pid ! {connected, self(), Ref, Socket},
            receive
                {transfer_socket, Pid, Ref} ->
                    ok = Mod:controlling_process(Socket, Pid),
                    Pid ! {ok, self(), Ref}
            after
                500 -> error(failed_to_transfer_ownership)
            end;
        {error, timeout} -> exit(timeout);
        {error, econnrefused} ->
            report_error(Address, {econnrefused, tcp, Port}),
            Pid ! {connection_error, self(), Ref, connection_refused};
        {error, ehostunreach} ->
            address_family_unavailable(AfType),
            Pid ! {connection_error, self(), Ref, bad_af};
        {error, enetunreach} ->
            address_family_unavailable(AfType),
            Pid ! {connection_error, self(), Ref, bad_af};
        {error, eafnosupport} ->
            address_family_unavailable(AfType),
            Pid ! {connection_error, self(), Ref, bad_af}
    end.


death_march(Owner, Reason, []) ->
    death_march(Owner, Reason);
death_march(Owner, Reason, [{_, _}|Rest]) ->
    death_march(Owner, Reason, Rest).


death_march(Owner, Reason) ->
    receive
        {send, _, _, _, _} ->
            death_march(Owner, Reason)
    after
        0 -> exit(normal)
    end.


send_queued(_Owner, Socket, State = #state{cbinfo={Mod,_,_,_}}, Queued) ->
    Fn = send_message(Socket, Mod),
    lists:foreach(Fn, lists:reverse([element(2, GenQueued) || GenQueued <- Queued])),
    Now = erlang:monotonic_time(millisecond),
    Pending = maps:from_list([{Id, {Pid, Now}} || {Pid, <<Id:16, _/binary>>} <- Queued]),
    loop(Socket, Pending, State).


loop(Socket, Pending, State = #state{pending=Pending, cbinfo={Mod,DataTag,ClosedTag,ErrTag}, address=Address}) ->
    receive
        {'EXIT', _, shutdown} ->
            ok = Mod:close(Socket),
            exit(normal);
        {send, Pid, Ref, _, <<Id:16, _binary>> = Bin} when byte_size(Bin) =< 16#FFFF ->
            Pid ! {send_ack, self(), Ref, Id},
            ok = Mod:send(Socket, [<<(byte_size(Bin)):16>>, Bin]),
            loop(Socket, Pending#{Id => {Pid, Ref, erlang:monotonic_time(millisecond)}}, State);
        {DataTag, Socket, Bin} ->
            case data(State, Bin) of
                {ok, State1} -> loop(Socket, Pending, State1);
                {error, _} -> Mod:close(Socket)
            end;
        {ClosedTag, Socket} ->
            Mod:close(Socket),
            % Should make sure that we got all expected responses
            % This process should never be restarted as it will not contain
            % the information necessary to execute the tasks entrusted to it...
            #state{address=Address,port=_Port} = State,
            case map_size(Pending) of
                0 -> ok;
                _ -> report_error(Address, missing_responses)
            end,
            exit(normal);
        {ErrTag, Socket, _Reason} ->
            Mod:close(Socket),
            % Should make sure that we got all expected responses
            #state{address=Address,port=Port} = State,
            case map_size(Pending) of
                0 -> ok;
                _ -> report_error(Address, missing_responses)
            end,
            report_error(Address, {transport_error, tcp, Port}),
            exit(normal)
    after
        10000 -> % How long should a socket linger?
            % If we have pending requests, count negatively towards the addresses rep
            Mod:close(Socket),
            case map_size(Pending) of
                0 -> ok;
                _ -> report_error(Address, missing_responses)
            end,
            exit(normal)
    end.


data(State, <<>>) ->
    {ok, State};
data(State = #state{length=undefined,buf=Buf0,address=Address}, Data) ->
    case <<Buf0/binary, Data/binary>> of
        <<0:16, _/binary>> ->
            report_error(Address, unsolicited_message),
            {error, close};
        <<Len:16, Tail/binary>> -> data(State#state{length=Len}, Tail);
        Buf -> data(State#state{buf=Buf}, <<>>)
    end;
data(State = #state{length=Length,buf=Buf,pending=Pending,address=Address,port=Port}, Data) ->
    case <<Buf/binary, Data/binary>> of
        <<Bin:Length/binary, Tail/binary>> ->
            <<Id:16, _/binary>> = Bin,
            case Pending of
                #{Id := {Pid, Ref, Sent}} ->
                    Pid ! {recurse_response, self(), Ref, Address, Port, erlang:monotonic_time(millisecond) - Sent, Bin},
                    data(State#state{buf= <<>>}, Tail);
                #{} ->
                    report_error(Address, unsolicited_message),
                    {error, close}
            end;
        NewBuf -> {ok, State#state{buf=NewBuf}}
    end.


send_message(Socket, Transport) ->
    fun (Bin) ->
        ok = Transport:send(Socket, [<<(byte_size(Bin)):16>>, Bin])
    end.
