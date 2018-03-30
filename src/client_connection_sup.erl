%%%-------------------------------------------------------------------
%% @doc kurremkarmerruk top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(client_connection_sup).

-behaviour(supervisor).
-export([init/1]).

%% API
-export([start_link/0,start_resolver/3]).

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
    {ok, { {simple_one_for_one, 10, 5}, [
        #{
            id       => nil,
            start    => {?MODULE, start_resolver, []},
            type     => worker,
            restart  => temporary,
            shutdown => 1000
        }
    ]} }.

%%====================================================================
%% Internal functions
%%====================================================================

start_resolver(Mod, Owner, Opts) ->
    Mod:start_link(Owner, Opts).
