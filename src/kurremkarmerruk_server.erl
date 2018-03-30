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
-module(kurremkarmerruk_server).

-behavior(gen_server).
-export([init/1,code_change/3,terminate/2,handle_call/3,handle_cast/2,handle_info/2]).

-export([start_link/0,namespaces/0,namespaces_id_keyed/0,bound_address_families/0]).

-record(state, {namespaces,namespaces_by_id,dns_rrs,udp_socket_fds=[],tcp_socket_fds=[],tls_socket_fds=[],bound_address_families}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


namespaces() ->
    gen_server:call(?MODULE, namespaces).


namespaces_id_keyed() ->
    gen_server:call(?MODULE, namespaces_id_keyed).


bound_address_families() ->
    gen_server:call(?MODULE, bound_address_families).


init(_) ->
    process_flag(trap_exit, true),
    Tab = ets:new(rr, [{read_concurrency, true}, named_table]),
    ets:new(client_requests, [named_table, public]),
    Config0 = application:get_all_env(kurremkarmerruk),
    Config = case proplists:get_value(config_file, Config0) of
        undefined -> Config0;
        {priv_file, App, Path} ->
            {ok, CaseConfig} = file:consult(filename:join(code:priv_dir(App), Path)),
            CaseConfig;
        ConfigPath when is_list(ConfigPath) ->
            {ok, CaseConfig} = file:consult(ConfigPath),
            CaseConfig
    end,
    Namespaces = namespace:compile(Config),
    {BoundAfsUDP, SocketList} = start_udp(Namespaces, Config),
    {BoundAfsTCP, TcpSocketList, _} = start_tcp(Namespaces, Config),
    {BoundAfsTLS, TlsSocketList, _} = case proplists:get_value(tls, Config) of
        true -> start_tls(Namespaces, Config);
        _ -> {[], [], []}
    end,
    {ok, #state{namespaces=Namespaces,namespaces_by_id=namespace:id_key(Namespaces),dns_rrs=Tab,udp_socket_fds=SocketList,tcp_socket_fds=TcpSocketList,tls_socket_fds=TlsSocketList,bound_address_families=make_sense_of_afs(BoundAfsUDP, BoundAfsTCP, BoundAfsTLS)}}.


code_change(_, State, _) ->
    {ok, State}.


terminate(_, #state{udp_socket_fds=UdpFds,tcp_socket_fds=TcpFds,tls_socket_fds=TlsFds}) ->
    lists:foreach(fun (Fd) -> socketfd:close(Fd) end, UdpFds),
    lists:foreach(fun (Fd) -> socketfd:close(Fd) end, TcpFds),
    lists:foreach(fun (Fd) -> socketfd:close(Fd) end, TlsFds),
    ok.


handle_call(namespaces, _, State = #state{namespaces=Namespaces}) ->
    {reply, Namespaces, State};
handle_call(namespaces_id_keyed, _, State = #state{namespaces_by_id=Namespaces}) ->
    {reply, Namespaces, State};
handle_call(bound_address_families, _, State = #state{bound_address_families=Afs}) ->
    {reply, Afs, State};
handle_call(_, _, State) ->
    {noreply, State}.


handle_cast(_, State) ->
    {noreply, State}.


handle_info(_, State) ->
    {noreply, State}.


start_udp(Namespaces, Config) ->
    Port = case proplists:get_value(udp_port, Config) of
        undefined -> proplists:get_value(port, Config);
        TmpPort -> TmpPort
    end,
    WorkerCount = proplists:get_value(udp_socket_workers, Config, 10),
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
        any -> init_any_udp(Namespaces, Port, WorkerCount);
        Addresses when is_list(Addresses) ->
            Fn = udp_map_fun(Namespaces, Port, WorkerCount),
            Sockets = lists:map(Fn, Addresses),
            {lists:usort(lists:map(fun (Tuple) -> case tuple_size(Tuple) of 4 -> inet; 8 -> inet6 end end, Addresses)), Sockets};
        Tuple when is_tuple(Tuple) ->
            Fn = udp_map_fun(Namespaces, Port, WorkerCount),
            case tuple_size(Tuple) of
                4 -> {[inet], [Fn(Tuple)]};
                8 -> {[inet6], [Fn(Tuple)]}
            end
    end.


init_any_udp(Namespaces, Port, WorkerCount) ->
    Fn = fun (Af) ->
        case socketfd:open(udp, Af, Port) of
            {ok, Fd} -> {true, Af, Fd};
            {error, {_, eafnosupport}} -> {false, Af, nil}
        end
    end,
    Sockets = lists:map(Fn, [inet, inet6]),
    Ret = lists:foldl(fun ({true, Af, Fd}, {Afs, Sockets}) ->
        SocketList = lists:map(fun (_) ->
            {ok, FdDup} = socketfd:dup(Fd),
            {ok, _} = supervisor:start_child(udp_socket_worker_sup, [Namespaces, FdDup, Af]),
            FdDup
        end, lists:seq(1,WorkerCount)),
        {[Af|Afs], lists:append(SocketList, Sockets)}
    end, {[], []}, [GenTuple || GenTuple <- Sockets, element(1, GenTuple)]),
    [ok = socketfd:close(element(3, GenTuple)) || GenTuple <- Sockets, element(1, GenTuple)],
    Ret.


udp_map_fun(Namespaces, Port, WorkerCount) ->
    fun
        ({Address, OverridePort}) ->
            {ok, Fd} = socketfd:open(udp, Address, OverridePort),
            Af = case tuple_size(Address) of
                4 -> inet;
                8 -> inet6
            end,
            SocketList = lists:map(fun (_) ->
                {ok, FdDup} = socketfd:dup(Fd),
                {ok, _} = supervisor:start_child(udp_socket_worker_sup, [Namespaces, FdDup, Af]),
                FdDup
            end, lists:seq(1,WorkerCount)),
            ok = socketfd:close(Fd),
            SocketList;
        (Address) when is_tuple(Address) ->
            {ok, Fd} = socketfd:open(udp, Address, Port),
            Af = case tuple_size(Address) of
                4 -> inet;
                8 -> inet6
            end,
            SocketList = lists:map(fun (_) ->
                {ok, FdDup} = socketfd:dup(Fd),
                {ok, _} = supervisor:start_child(udp_socket_worker_sup, [Namespaces, FdDup, Af]),
                FdDup
            end, lists:seq(1,WorkerCount)),
            ok = socketfd:close(Fd),
            SocketList
    end.



start_tcp(Namespaces, Config) ->
    Port = case proplists:get_value(tcp_port, Config) of
        undefined -> proplists:get_value(port, Config);
        TmpPort -> TmpPort
    end,
    case
        case proplists:get_value(tcp_address, Config) of
            undefined ->
                case proplists:get_value(address, Config) of
                    undefined -> any;
                    TmpAddress -> TmpAddress
                end;
            TmpAddress -> TmpAddress
        end
    of
        % Should we feed namespace stuff to handlers here?
        any -> init_any_tcp(Namespaces, Port);
        Addresses when is_list(Addresses) ->
            Pids = lists:map(map_stream_fun(ranch_tcp, [], Port, Namespaces), Addresses),
            {lists:usort(lists:map(fun (Tuple) -> case tuple_size(Tuple) of 4 -> inet; 8 -> inet6 end end, Addresses)), Pids};
        Address when is_tuple(Address) ->
            Fn = map_stream_fun(ranch_tcp, [], Port, Namespaces),
            case tuple_size(Address) of
                4 -> {[inet], [Fn(Address)]};
                8 -> {[inet6], [Fn(Address)]}
            end
    end.


init_any_tcp(Namespaces, Port) ->
    Fn = fun ({Af, ListenerName}) ->
        case socketfd:open(tcp, Af, Port) of
            {ok, Fd} -> {true, Af, Fd, ListenerName};
            {error, {_, eafnosupport}} -> {false, Af, nil, nil}
        end
    end,
    Sockets = lists:map(Fn, [{inet, dns_tcp_inet}, {inet6, dns_tcp_inet6}]),
    lists:foldl(fun ({true, Af, Fd, ListenerName}, {Afs, Fds, Pids}) ->
        {ok, Pid} = ranch:start_listener(ListenerName, ranch_tcp, [Af, {fd, socketfd:get(Fd)}], dns_stream_protocol, [Namespaces]),
        {[Af|Afs], [Fd|Fds], [Pid|Pids]}
    end, {[], [], []}, [GenTuple || GenTuple <- Sockets, element(1, GenTuple)]).


start_tls(Namespaces, Config) ->
    {ok, Port} = proplists:get_value(tls_port, Config),
    TLSOpts = [],
    case
        case proplists:get_value(tcp_address, Config) of
            undefined ->
                case proplists:get_value(address, Config) of
                    undefined -> any;
                    TmpAddress -> TmpAddress
                end;
            TmpAddress -> TmpAddress
        end
    of
        % Should we feed namespace stuff to handlers here?
        any -> init_any_tls(Namespaces, Port, TLSOpts);
        Addresses when is_list(Addresses) ->
            Pids = lists:map(map_stream_fun(ranch_ssl, TLSOpts, Port, Namespaces), Addresses),
            {lists:usort(lists:map(fun (Tuple) -> case tuple_size(Tuple) of 4 -> inet; 8 -> inet6 end end, Addresses)), Pids};
        Address when is_tuple(Address) ->
            Fn = map_stream_fun(ranch_ssl, TLSOpts, Port, Namespaces),
            case tuple_size(Address) of
                4 -> {[inet], [Fn(Address)]};
                8 -> {[inet6], [Fn(Address)]}
            end
    end.


init_any_tls(Namespaces, Port, TLSOpts) ->
    Fn = fun ({Af, ListenerName}) ->
        case socketfd:open(tcp, Af, Port) of
            {ok, Fd} -> {true, Af, Fd, ListenerName};
            {error, {_, eafnosupport}} -> {false, Af, nil, nil}
        end
    end,
    Sockets = lists:map(Fn, [{inet, dns_tls_inet}, {inet6, dns_tls_inet6}]),
    lists:foldl(fun ({true, Af, Fd, ListenerName}, {Afs, Fds, Pids}) ->
        {ok, Pid} = ranch:start_listener(ListenerName, ranch_ssl, [Af, {fd, socketfd:get(Fd)}|TLSOpts], dns_stream_protocol, [Namespaces]),
        {[Af|Afs], [Fd|Fds], [Pid|Pids]}
    end, {[], [], []}, [GenTuple || GenTuple <- Sockets, element(1, GenTuple)]).


map_stream_fun(Transport, TransportOptsBase, Port, Namespaces) ->
    fun
        ({Address = {_, _, _, _, _, _, _, _}, OverridePort} = Tuple) ->
            TransportOpts = [{port, OverridePort}, {ip, Address}, {ipv6_v6only, true}|TransportOptsBase],
            {ok, Pid} = ranch:start_listener(Tuple, Transport, TransportOpts, dns_stream_protocol, [Namespaces]),
            Pid;
        ({Address = {_, _, _, _}, OverridePort} = Tuple) ->
            TransportOpts = [{port, OverridePort}, {ip, Address}|TransportOptsBase],
            {ok, Pid} = ranch:start_listener(Tuple, Transport, TransportOpts, dns_stream_protocol, [Namespaces]),
            Pid;
        (Address = {_, _, _, _, _, _, _, _}) ->
            TransportOpts = [{port, Port}, {ip, Address}, {ipv6_v6only, true}|TransportOptsBase],
            {ok, Pid} = ranch:start_listener({Address, Port}, Transport, TransportOpts, dns_stream_protocol, [Namespaces]),
            Pid;
        (Address = {_, _, _, _}) ->
            TransportOpts = [{port, Port}, {ip, Address}|TransportOptsBase],
            {ok, Pid} = ranch:start_listener({Address, Port}, Transport, TransportOpts, dns_stream_protocol, [Namespaces]),
            Pid
    end.


make_sense_of_afs(L1, L2, L3) ->
    lists:usort(lists:append([L1, L2, L3])).
