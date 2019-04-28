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
-module(udp_client).

% Generalize this somehow, so that it's possible to use the same base in recurse and
% transfer

-export([start_link/2, init/2, loop/3]).

-record(state, {owner, timeout=timer:seconds(30), type}).

-include_lib("kernel/include/logger.hrl").

-import(kurremkarmerruk_recurse_server, [
    address_family_unavailable/1
]).

-import(kurremkarmerruk_recurse_address_reputation, [
    update_address_reputation/3,
    report_error/2
]).


start_link(Owner, Opts) ->
    Pid = spawn_link(?MODULE, init, [Owner, Opts]),
    {ok, Pid}.


init(Owner, #{socket_type := Type}) ->
    % Do we need the Type parameter?
    process_flag(trap_exit, true),
    case gen_udp:open(0, [{active, true}, binary, Type]) of
        {ok, Socket} -> loop(Socket, #{}, #state{type=Type, owner=Owner});
        {error, eafnosupport} ->
            address_family_unavailable(Type),
            death_march(eafnosupport)
    end.


loop(Socket, Pending, State = #state{timeout=_Timeout, type=Type}) ->
    receive
        {'EXIT', _, shutdown} -> ok = gen_udp:close(Socket);
        {send, Pid, Ref, {Address, Port}, <<Id:16, _/binary>> = Bin} ->
            Pid ! {send_ack, self(), Ref, Id},
            case gen_udp:send(Socket, Address, Port, Bin) of
                ok -> loop(Socket, Pending#{{Id, Address, Port} => {Pid, Ref, erlang:monotonic_time(millisecond)}}, State);
                {error, ehostunreach} ->
                    address_family_unavailable(Type),
                    Pid ! {recurse_reroute, Ref, Id, bad_af},
                    death_march(ehostunreach);
                {error, enetunreach} ->
                    address_family_unavailable(Type),
                    Pid ! {recurse_reroute, Ref, Id, bad_af},
                    death_march(enetunreach);
                ErrTuple ->
                    Pid ! {recurse_reroute, Ref, Id, bad_af},
                    ?LOG_ERROR("~p: Error trying to send message: ~p", [?MODULE, ErrTuple]),
                    loop(Socket, Pending, State)
            end;
        {udp, Socket, Address, Port, Bin = <<Id:16, _/binary>>} ->
            Key = {Id, Address, Port},
            case Pending of
                % Do we require that the id should match the address and port?
                #{Key := {Pid, Ref, Sent}} ->
                    Pid ! {recurse_response, self(), Ref, Address, Port, erlang:monotonic_time(millisecond) - Sent, Bin},
                    loop(Socket, maps:remove(Key, Pending), State);
                #{} ->
                    report_error(Address, unsolicited_message),
                    loop(Socket, Pending, State)
            end;
        {udp, Socket, _Address, _Port, _Bin} ->
            % Log ufo? Decrease address rep?
            loop(Socket, Pending, State)
    end.


death_march(Reason) ->
    receive
        {send, _Pid, _, _} ->
            death_march(Reason)
    after
        0 -> ok
    end.
