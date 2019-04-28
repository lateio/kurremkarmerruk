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
% Create socketfds based on which udp_socket_workers
% will be created
%
-module(udp_socket_control_server).

-behavior(gen_server).
-export([init/1,code_change/3,terminate/2,handle_call/3,handle_cast/2,handle_info/2]).

-export([start_link/0]).

-export([socketfds/0]).

-include_lib("kernel/include/logger.hrl").

-record(state, {socketfds=[]}).

socketfds() ->
    gen_server:call(?MODULE, socketfds).


start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


init(_) ->
    process_flag(trap_exit, true),
    try start_udp(kurremkarmerruk_server:namespaces(), kurremkarmerruk_server:config()) of
        {_, SocketList} -> {ok, #state{socketfds=SocketList}}
    catch
        abort ->
            init:stop(1),
            {ok, #state{}}
    end.


code_change(_, State, _) ->
    {ok, State}.


terminate(_, #state{socketfds=Sockets}) ->
    [socketfd:close(GenSocketfd) || GenSocketfd <- Sockets],
    ok.


%namespace_change -> use ranch:set_protocol_options() to update namespaces on new connections
handle_call(socketfds, _, State = #state{socketfds=Sockets}) ->
    {reply, Sockets, State};
handle_call(_, _, State) ->
    {noreply, State}.


handle_cast(_, State) ->
    {noreply, State}.


handle_info(_, State) ->
    {noreply, State}.


start_udp(Namespaces, Config) ->
    Port = case proplists:get_value(udp_port, Config) of
        undefined -> proplists:get_value(port, Config, 53);
        TmpPort -> TmpPort
    end,
    WorkerCount = case proplists:get_value(udp_socket_workers, Config, 10) of
        CaseCount when is_integer(CaseCount), CaseCount > 0 -> CaseCount
    end,
    case
        case proplists:get_value(udp_address, Config) of
            undefined ->
                case proplists:get_value(address, Config) of
                    undefined -> any;
                    TmpAddress -> TmpAddress
                end;
            TmpAddress -> TmpAddress
        end
    of
        any ->
            case init_any_udp(Namespaces, Port, WorkerCount) of
                {[], []} ->
                    ?LOG_ERROR("UDP Sockets: No address(es) specified and failed to bind either 0.0.0.0:~B or [::]:~B", [Port, Port]),
                    throw(abort);
                {Afs, _} = Tuple ->
                    {Format, ArgCount} = case {lists:member(inet, Afs), lists:member(inet6, Afs)} of
                        {true, true} -> {"UDP Sockets: No address(es) specified, bound 0.0.0.0:~B and [::]:~B", 2};
                        {true, false} -> {"UDP Sockets: No address(es) specified, bound 0.0.0.0:~B", 1};
                        {false, true} -> {"UDP Sockets: No address(es) specified, bound [::]:~B", 1}
                    end,
                    ?LOG_INFO(Format, [Port || _ <- lists:seq(1, ArgCount)]),
                    Tuple
            end;
        false ->
            ?LOG_INFO("UDP Sockets: Binding disabled"),
            {[], []};
        [] ->
            ?LOG_INFO("UDP Sockets: Binding disabled"),
            {[], []};
        [_|_] = Address when is_integer(hd(Address)) ->
            Fn = udp_map_fun(Namespaces, Port, WorkerCount),
            case Fn(Address) of
                [] -> throw(abort);
                SocketList -> {lists:usort(lists:map(fun (FunFd) -> socketfd:address_family(FunFd) end, SocketList)), SocketList}
            end;
        Addresses when is_list(Addresses) ->
            Fn = udp_map_fun(Namespaces, Port, WorkerCount),
            Sockets = lists:map(Fn, Addresses),
            SocketList = lists:append(Sockets),
            case lists:all(fun (FunSocketList) -> FunSocketList =/= [] end, Sockets) of
                true -> {lists:usort(lists:map(fun (FunFd) -> socketfd:address_family(FunFd) end, SocketList)), SocketList};
                false -> throw(abort)
            end;
        Tuple when is_tuple(Tuple) ->
            Fn = udp_map_fun(Namespaces, Port, WorkerCount),
            case Fn(Tuple) of
                [] -> throw(abort);
                SocketList when tuple_size(Tuple) =:= 4 -> {[inet], SocketList};
                SocketList when tuple_size(Tuple) =:= 8 -> {[inet6], SocketList}
            end
    end.


init_any_udp(_, Port, WorkerCount) ->
    Fn = fun (Af) ->
        case socketfd:open(udp, Port, Af) of
            {ok, Fd} -> {true, Af, Fd};
            {error, Reason} ->
                Format = case Af of
                    inet  -> "Failed to bind UDP port 0.0.0.0:~B: ~p";
                    inet6 ->"Failed to bind UDP port [::]:~B: ~p"
                end,
                ?LOG_WARNING(Format, [Port, Reason]),
                {false, Af, nil}
        end
    end,
    Sockets = lists:map(Fn, [inet, inet6]),
    Ret = lists:foldl(fun ({true, Af, Fd}, {Afs, FunSockets}) ->
        SocketList = lists:map(fun (_) ->
            {ok, FdDup} = socketfd:dup(Fd),
            FdDup
        end, lists:seq(1,WorkerCount)),
        {[Af|Afs], lists:append(SocketList, FunSockets)}
    end, {[], []}, [GenTuple || GenTuple <- Sockets, element(1, GenTuple)]),
    [ok = socketfd:close(element(3, GenTuple)) || GenTuple <- Sockets, element(1, GenTuple)],
    Ret.


udp_map_fun(_, Port, WorkerCount) ->
    fun
        ({Address, OverridePort}) ->
            case socketfd:open(udp, OverridePort, Address) of
                {ok, Fd} ->
                    SocketList = lists:map(fun (_) ->
                        {ok, FdDup} = socketfd:dup(Fd),
                        FdDup
                    end, lists:seq(1,WorkerCount)),
                    ok = socketfd:close(Fd),
                    SocketList;
                {error, Reason} ->
                    Format = case tuple_size(Address) of
                        4 -> "Failed to bind UDP port ~s:~B: ~p";
                        8 -> "Failed to bind UDP port [~s:]~B: ~p"
                    end,
                    ?LOG_ERROR(Format, [inet:ntoa(Address), OverridePort, Reason]),
                    []
            end;
        ([_|_] = Address0) when is_integer(hd(Address0)) ->
            case inet:parse_address(Address0) of
                {ok, Address1} ->
                    case socketfd:open(udp, Port, Address1) of
                        {ok, Fd} ->
                            SocketList = lists:map(fun (_) ->
                                {ok, FdDup} = socketfd:dup(Fd),
                                FdDup
                            end, lists:seq(1,WorkerCount)),
                            ok = socketfd:close(Fd),
                            SocketList;
                        {error, Reason} ->
                            Format = case tuple_size(Address1) of
                                4 -> "Failed to bind UDP port ~s:~B: ~p";
                                8 -> "Failed to bind UDP port [~s]:~B: ~p"
                            end,
                            ?LOG_ERROR(Format, [inet:ntoa(Address1), Port, Reason]),
                            []
                    end;
                _ ->
                    ?LOG_ERROR("Not a valid ip address: ~s", [Address0]),
                    []
            end;
        (Address) when is_tuple(Address) ->
            case socketfd:open(udp, Port, Address) of
                {ok, Fd} ->
                    SocketList = lists:map(fun (_) ->
                        {ok, FdDup} = socketfd:dup(Fd),
                        FdDup
                    end, lists:seq(1,WorkerCount)),
                    ok = socketfd:close(Fd),
                    SocketList;
                {error, Reason} ->
                    Format = case tuple_size(Address) of
                        4 -> "Failed to bind UDP port ~s:~B: ~p";
                        8 -> "Failed to bind UDP port [~s]:~B: ~p"
                    end,
                    ?LOG_ERROR(Format, [inet:ntoa(Address), Port, Reason]),
                    []
            end
    end.
