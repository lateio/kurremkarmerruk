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
% Bring ranch listeners up and down
%
-module(stream_socket_control_server).

-behavior(gen_server).
-export([init/1,code_change/3,terminate/2,handle_call/3,handle_cast/2,handle_info/2]).

-export([start_link/0]).

-include_lib("kernel/include/logger.hrl").

-record(state, {tcp_refs=[],tls_refs=[]}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


init(_) ->
    process_flag(trap_exit, true),
    Config = kurremkarmerruk_server:config(),
    Namespaces = kurremkarmerruk_server:namespaces(),
    try
        {_, TcpRefs} = start_tcp(Namespaces, Config),
        {_, TlsRefs} = case proplists:get_value(tls, Config) of
            undefined -> {[], []};
            _ -> start_tls(Namespaces, Config)
        end,
        {ok, #state{tcp_refs=TcpRefs,tls_refs=TlsRefs}}
    catch
        abort ->
            init:stop(1),
            {ok, #state{}}
    end.


code_change(_, State, _) ->
    {ok, State}.


terminate(_, #state{tcp_refs=Refs1,tls_refs=Refs2}) ->
    true = lists:all(fun (FunRef) -> ok =:= ranch:stop_listener(FunRef) end, lists:append(Refs1, Refs2)),
    ok.


%namespace_change -> use ranch:set_protocol_options() to update namespaces on new connections
handle_call(_, _, State) ->
    {noreply, State}.


handle_cast(_, State) ->
    {noreply, State}.


handle_info(_, State) ->
    {noreply, State}.


start_tcp(Namespaces, Config) ->
    Port = case proplists:get_value(tcp_port, Config) of
        undefined -> proplists:get_value(port, Config, 53);
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
        any ->
            case init_any_tcp(Namespaces, Port) of
                {[], []} ->
                    ?LOG_ERROR("TCP Sockets: No address(es) specified and failed to bind either 0.0.0.0:~B or [::]:~B", [Port, Port]),
                    throw(abort);
                {_, TcpRefs} = Tuple ->
                    {Format, ArgCount} = case {lists:member(dns_tcp_inet, TcpRefs), lists:member(dns_tcp_inet6, TcpRefs)} of
                        {true, true} -> {"TCP Sockets: No address(es) specified, bound 0.0.0.0:~B and [::]:~B", 2};
                        {true, false} -> {"TCP Sockets: No address(es) specified, bound 0.0.0.0:~B", 1};
                        {false, true} -> {"TCP Sockets: No address(es) specified, bound [::]:~B", 1}
                    end,
                    ?LOG_INFO(Format, [Port || _ <- lists:seq(1, ArgCount)]),
                    Tuple
            end;
        false ->
            ?LOG_INFO("TCP Sockets: Binding disabled"),
            {[], []};
        [] ->
            ?LOG_INFO("TCP Sockets: Binding disabled"),
            {[], []};
        [_|_] = Address when is_integer(hd(Address)) ->
            Fn = fold_stream_fun(ranch_tcp, [], Port, Namespaces),
            case Fn(Address, []) of
                [] -> throw(abort);
                [{{_, _, _, _}, _} = Ref] -> {[inet], [Ref]};
                [{{_, _, _, _, _, _, _, _}, _} = Ref] -> {[inet6], [Ref]}
            end;
        Addresses when is_list(Addresses) ->
            Refs = lists:foldl(fold_stream_fun(ranch_tcp, [], Port, Namespaces), [], Addresses),
            case length(Refs) < length(Addresses) of
                true  -> throw(abort);
                false -> {lists:usort(lists:map(fun ({Tuple, _}) -> case tuple_size(Tuple) of 4 -> inet; 8 -> inet6 end end, Refs)), Refs}
            end;
        Address when is_tuple(Address) ->
            Fn = fold_stream_fun(ranch_tcp, [], Port, Namespaces),
            case Fn(Address, []) of
                [] -> throw(abort);
                Refs ->
                    case tuple_size(Address) of
                        4 -> {[inet],  Refs};
                        8 -> {[inet6], Refs}
                    end
            end
    end.


init_any_tcp(Namespaces, Port) ->
    lists:foldl(fun ({Ref, Af, Tail}, {Afs, Refs}) ->
        case ranch:start_listener(Ref, ranch_tcp, [Af, {port, Port}|Tail], stream_worker_protocol, [Namespaces]) of
            {ok, _}  -> {[Af|Afs], [Ref|Refs]};
            {error, Error} ->
                Format = case Af of
                    inet -> "Failed to bind TCP port 0.0.0.0:~B: ~p";
                    inet6 -> "Failed to bind TCP port [::]:~B: ~p"
                end,
                ?LOG_WARNING(Format, [Port, Error]),
                {Afs, Refs}
        end
    end, {[], []}, [{dns_tcp_inet, inet, []}, {dns_tcp_inet6, inet6, [{ipv6_v6only, true}]}]).


start_tls(Namespaces, Config) ->
    Port = proplists:get_value(tls_port, Config, 853),
    _TlsConfig = proplists:get_value(tls, Config),
    % Get certfile, some other options...
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
        any ->
            ?LOG_INFO("TLS Sockets: No address specified, binding 0.0.0.0:~B and [::]:~B", [Port, Port]),
            case init_any_tls(Namespaces, Port, TLSOpts) of
                {[], []} ->
                    ?LOG_ERROR("TCP Sockets: No address(es) specified and failed to bind either 0.0.0.0:~B or [::]:~B", [Port, Port]),
                    throw(abort);
                {_, TcpRefs} = Tuple ->
                    {Format, ArgCount} = case {lists:member(dns_tcp_inet, TcpRefs), lists:member(dns_tcp_inet6, TcpRefs)} of
                        {true, true} -> {"TLS Sockets: No address(es) specified, bound 0.0.0.0:~B and [::]:~B", 2};
                        {true, false} -> {"TLS Sockets: No address(es) specified, bound 0.0.0.0:~B", 1};
                        {false, true} -> {"TLS Sockets: No address(es) specified, bound [::]:~B", 1}
                    end,
                    ?LOG_INFO(Format, [Port || _ <- lists:seq(1, ArgCount)]),
                    Tuple
            end;
        false ->
            ?LOG_INFO("TLS Sockets: Binding disabled"),
            {[], []};
        Addresses when is_list(Addresses) ->
            Refs = lists:foldl(fold_stream_fun(ranch_ssl, TLSOpts, Port, Namespaces), [], Addresses),
            case length(Refs) < length(Addresses) of
                true  -> throw(abort);
                false -> {lists:usort(lists:map(fun (Tuple) -> case tuple_size(Tuple) of 4 -> inet; 8 -> inet6 end end, Addresses)), Refs}
            end;
        Address when is_tuple(Address) ->
            Fn = fold_stream_fun(ranch_ssl, TLSOpts, Port, Namespaces),
            case Fn(Address, []) of
                [] -> throw(abort);
                Refs ->
                    case tuple_size(Address) of
                        4 -> {[inet],  Refs};
                        8 -> {[inet6], Refs}
                    end
            end
    end.


init_any_tls(Namespaces, Port, TLSOpts) ->
    lists:foldl(fun ({Ref, Af, Tail}, {Afs, Refs}) ->
        case ranch:start_listener(Ref, ranch_ssl, [Af, {port, Port}|Tail], stream_worker_protocol, [Namespaces]) of
            {ok, _}  -> {[Af|Afs], [Ref|Refs]};
            {error, _} -> {Afs, Refs}
        end
    end, {[], []}, [{dns_tls_inet, inet, TLSOpts}, {dns_tls_inet6, inet6, [{ipv6_v6only, true}|TLSOpts]}]).


fold_stream_fun(Transport, TransportOptsBase, Port, Namespaces) ->
    fun
        ({Address = {_, _, _, _, _, _, _, _}, OverridePort} = Ref, Acc) ->
            TransportOpts = [{port, OverridePort}, {ip, Address}, {ipv6_v6only, true}|TransportOptsBase],
            case ranch:start_listener(Ref, Transport, TransportOpts, stream_worker_protocol, [Namespaces]) of
                {ok, _} -> [Ref|Acc];
                {error, Reason} ->
                    ?LOG_ERROR("Failed to bind [~s]:~B: ~p", [inet:ntoa(Address), OverridePort, Reason]),
                    Acc
            end;
        ({Address = {_, _, _, _}, OverridePort} = Ref, Acc) ->
            TransportOpts = [{port, OverridePort}, {ip, Address}|TransportOptsBase],
            case ranch:start_listener(Ref, Transport, TransportOpts, stream_worker_protocol, [Namespaces]) of
                {ok, _} -> [Ref|Acc];
                {error, Reason} ->
                    ?LOG_ERROR("Failed to bind ~s:~B: ~p", [inet:ntoa(Address), OverridePort, Reason]),
                    Acc
            end;
        (Address = {_, _, _, _, _, _, _, _}, Acc) ->
            Ref = {Address, Port},
            TransportOpts = [{port, Port}, {ip, Address}, {ipv6_v6only, true}|TransportOptsBase],
            case ranch:start_listener(Ref, Transport, TransportOpts, stream_worker_protocol, [Namespaces]) of
                {ok, _} -> [Ref|Acc];
                {error, Reason} ->
                    ?LOG_ERROR("Failed to bind [~s]:~B: ~p", [inet:ntoa(Address), Port, Reason]),
                    Acc
            end;
        (Address = {_, _, _, _}, Acc) ->
            Ref = {Address, Port},
            TransportOpts = [{port, Port}, {ip, Address}|TransportOptsBase],
            case ranch:start_listener(Ref, Transport, TransportOpts, stream_worker_protocol, [Namespaces]) of
                {ok, _} -> [Ref|Acc];
                {error, Reason} ->
                    ?LOG_ERROR("Failed to bind ~s:~B: ~p", [inet:ntoa(Address), Port, Reason]),
                    Acc
            end;
        (Address0 = [_|_], Acc) when is_integer(hd(Address0)) ->
            case inet:parse_address(Address0) of
                {ok, Address1} ->
                    Ref = {Address1, Port},
                    TransportOpts = [{port, Port}, {ip, Address1}|TransportOptsBase],
                    case ranch:start_listener(Ref, Transport, TransportOpts, stream_worker_protocol, [Namespaces]) of
                        {ok, _} -> [Ref|Acc];
                        {error, Reason} ->
                            ?LOG_ERROR("Failed to bind ~s:~B: ~p", [inet:ntoa(Address1), Port, Reason]),
                            Acc
                    end;
                _ ->
                    ?LOG_ERROR("Invalid address ~s", [Address0]),
                    Acc
            end
    end.
