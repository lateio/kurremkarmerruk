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
-module(kurremkarmerruk_recurse_server).

-behavior(gen_server).
-export([init/1,code_change/3,terminate/2,handle_call/3,handle_cast/2,handle_info/2]).


-export([
    start_link/0,
    return_result/2,
    message_timeout/1,
    ack_message_send/2,
    reroute_message_send/3,
    reroute_message/2,
    message_requires_tcp/1,

    address_family_unavailable/1,
    connect/1,
    udp_socket/1
]).

-include_lib("kurremkarmerruk/include/recurse.hrl").

-define(SIN_LIMIT, 20).


% Timeout behavior state?
-record(server_state, {
    tab_msg,
    resolvers=#{}
}).


init([]) ->
    process_flag(trap_exit, true),
    TabMsg = ets:new(dns_resolver, []),
    {ok, #server_state{tab_msg=TabMsg}}.


code_change(_OldVsn, _State, _Extra) ->
    {ok, _State}.


terminate(_Reason, _State) ->
    ok.


prepare_resolve({Type, AddressTuple={Address, Port}}) ->
    {
        {Type, AddressTuple},
        stream_client,
        #{
            address => Address,
            port    => Port,
            cbinfo  =>
                case Type of
                    tcp -> {gen_tcp,tcp,tcp_closed,tcp_error};
                    tls -> {ssl,ssl,ssl_closed,ssl_error}
                end
        }
    }.


handle_call({connect, {ServerSpec, _}}, _, State = #server_state{resolvers=Resolvers}) ->
    case Resolvers of
        #{ServerSpec := {Pid, _}} -> {reply, {ok, Pid}, State};
        #{} ->
            {_, Module, Opts} = prepare_resolve(ServerSpec),
            {ok, Pid} = supervisor:start_child(kurremkarmerruk_recurse_client_sup, [Module, self(), Opts]),
            Mon = monitor(process, Pid), % What if there's a loop of worker spawns-downs for some reason?
            {reply, {ok, Pid}, State#server_state{resolvers=Resolvers#{ServerSpec => {Pid, Mon}}}}
    end;
handle_call({udp_socket, Af}, _, State) ->
    Name = case Af of
        inet  -> kurremkarmerruk_recurse_udp_inet;
        inet6 -> kurremkarmerruk_recurse_udp_inet6
    end,
    % It turns out that trying to register fairly volatile pids as atoms is somewhat troublesome...
    % Need to come up with a better way of specifying sockets for workers.
    case whereis(Name) of
        undefined ->
            {ok, Pid} = supervisor:start_child(kurremkarmerruk_recurse_client_sup, [udp_client, self(), #{socket_type => Af}]),
            true = register(Name, Pid),
            {reply, {ok, Pid}, State};
        Pid -> {reply, {ok, Pid}, State}
    end;
handle_call(_, _, State) ->
    {noreply, State}.


handle_cast({address_family_unavailable, Af, Pid}, State = #server_state{resolvers=Resolvers0}) ->
    % Remove resolver to stop messages being sent to it.
    Resolvers1 = case lists:filter(fun ({_, Tuple}) -> element(1, Tuple) =:= Pid end, maps:to_list(Resolvers0)) of
        [] -> Resolvers0;
        [{Key, _}] -> maps:remove(Key, Resolvers0)
    end,
    kurremkarmerruk_recurse_address_reputation:address_family_unavailable(Af),
    {noreply, State#server_state{resolvers=Resolvers1}};
handle_cast({msg, Msg}, State = #server_state{tab_msg=Tab}) ->
    case ets:take(Tab, dnsmsg:id(Msg)) of
        [] -> ok;
        [{_, Pid, Ref}] -> Pid ! {recurse_response, Ref, Msg}
    end,
    {noreply, State};
handle_cast({timeout, Id}, State = #server_state{tab_msg=Tab}) ->
    case ets:take(Tab, Id) of
        [] -> ok;% It could be that lookup returns nothing, as the message has already received a response
        [{Id, Pid, Ref}] -> Pid ! {recurse_timeout, Ref, Id}
    end,
    {noreply, State};
handle_cast({reroute_message, Id, Reason}, State = #server_state{tab_msg=Tab}) ->
    case ets:take(Tab, Id) of
        [{_, Pid, Ref}] -> Pid ! {recurse_reroute, Ref, Id, Reason}
    end,
    {noreply, State};
handle_cast({message_requires_tcp, Id}, State = #server_state{tab_msg=Tab}) ->
    case ets:lookup(Tab, Id) of
        [{Id, Pid, Ref}] -> Pid ! {recurse_requires_tcp, Ref, Id}
    end,
    {noreply, State};
handle_cast(_Msg, _State) ->
    {noreply, _State}.


handle_info({'DOWN', Mon, process, Resolver, _}, State) ->
    {noreply, handle_worker_down(Resolver, Mon, State)};
handle_info(_Msg, _State) ->
    {noreply, _State}.


start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


return_result(Pid, Msg) ->
    gen_server:cast(Pid, {msg, Msg}).


message_timeout(Id) ->
    gen_server:cast(?MODULE, {timeout, Id}).


handle_worker_down(Pid, Ref, State = #server_state{resolvers=Resolvers0}) ->
    Resolvers1 = case lists:filter(fun ({_, Tuple}) -> Tuple =:= {Pid, Ref} end, maps:to_list(Resolvers0)) of
        [] -> Resolvers0;
        [{Key, _}] -> maps:remove(Key, Resolvers0)
    end,
    State#server_state{resolvers=Resolvers1}.


ack_message_send(Pid, Ref) ->
    Pid ! {send_ack, self(), Ref}.


reroute_message_send(Pid, Ref, Reason) ->
    Pid ! {send_reroute, self(), Ref, Reason}.


connect(ServerSpecOpts) ->
    gen_server:call(?MODULE, {connect, ServerSpecOpts}).


udp_socket(Af) ->
    gen_server:call(?MODULE, {udp_socket, Af}).


reroute_message(Msg, Reason) when is_map(Msg) ->
    gen_server:cast(?MODULE, {reroute_message, dnsmsg:id(Msg), Reason});
reroute_message(Id, Reason) when is_integer(Id) ->
    gen_server:cast(?MODULE, {reroute_message, Id, Reason}).


message_requires_tcp(Id) ->
    gen_server:cast(?MODULE, {message_requires_tcp, Id}).


address_family_unavailable(Af) ->
    gen_server:cast(?MODULE, {address_family_unavailable, Af, self()}).
