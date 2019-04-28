%%%-------------------------------------------------------------------
%% @doc kurremkarmerruk top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(kurremkarmerruk_sup).

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
%    supervisor:start_link({global, ?SERVER}, ?MODULE, []).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([]) ->
    {ok, { {rest_for_one, 2, 2}, [
        #{
            id => handlers_sup,
            start => {kurremkarmerruk_handlers_sup, start_link, []},
            type => supervisor,
            restart => permanent,
            shutdown => 5000
        },
        #{
            id => server,
            start => {kurremkarmerruk_server, start_link, []},
            type => worker,
            restart => permanent,
            shutdown => 2000
        },
        #{
            id => socket_sup,
            start => {socket_sup, start_link, []},
            type => supervisor,
            restart => permanent,
            shutdown => 5000
        }
    ]} }.

%%====================================================================
%% Internal functions
%%====================================================================
