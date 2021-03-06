%%%-------------------------------------------------------------------
%% @doc kurremkarmerruk top level supervisor.
%% @end
%%%-------------------------------------------------------------------

% Split udp socket pool from kurremkarmerruk, somewhat like ranch?

-module(udp_socket_worker_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([]) ->
    Sockets = udp_socket_control_server:socketfds(),
    ChildSpecs = [
        #{
            id => {udp_worker, socketfd:get(GenSocketfd)},
            start => {udp_socket_worker, start_link, [GenSocketfd]},
            type => worker,
            restart => permanent,
            shutdown => 2000
        }
        || GenSocketfd <- Sockets
    ],
    {ok, {{one_for_one, 2, 2}, ChildSpecs}}.

%%====================================================================
%% Internal functions
%%====================================================================
