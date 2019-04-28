%%%-------------------------------------------------------------------
%% @doc kurremkarmerruk top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(socket_sup).

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
    {ok, { {one_for_one, 2, 2}, [
        #{
            id => stream_socket_control_server,
            start => {stream_socket_control_server, start_link, []},
            type => worker,
            restart => permanent,
            shutdown => 2000
        },
        #{
            id => udp_sup,
            start => {udp_sup, start_link, []},
            type => supervisor,
            restart => permanent,
            shutdown => 5000
        }
    ]} }.

%%====================================================================
%% Internal functions
%%====================================================================
