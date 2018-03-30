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
    %create_ets_tables(),
    {ok, { {rest_for_one, 2, 2}, [
        #{
            id => udp_socket_worker_sup,
            start => {udp_socket_worker_sup, start_link, []},
            type => supervisor,
            restart => permanent,
            shutdown => 5000
        },
        #{
            id => pool_worker_sup,
            start => {pool_worker_sup, start_link, []},
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
            id => client_connection_sup,
            start => {client_connection_sup, start_link, []},
            type => supervisor,
            restart => permanent,
            shutdown => 5000
        },
        #{
            id => cache,
            start => {kurremkarmerruk_cache, start_link, []},
            type => worker,
            restart => permanent,
            shutdown => 2000
        },
        #{
            id => recurse,
            start => {kurremkarmerruk_recurse, start_link, []},
            type => worker,
            restart => permanent,
            shutdown => 2000
        }
    ]} }.

%%====================================================================
%% Internal functions
%%====================================================================

%create_ets_tables() ->
    % Create ets tables here to protect them from process terminations
    %Tables = [
        % Creating rr/cache tables here prevents us from moving to data storage being a behavior
    %    {kurremkarmerruk_rr, [{read_concurrency, true}]},
    %    {kurremkarmerruk_cache, [{read_concurrency, true}]},
    %    {kurremkarmerruk_resolver, []},
    %    {kurremkarmerruk_resolver_timeouts, []}
    %],
    %Fn = fun ({Name, Tail}) -> ets:new(Name, [named_table, public|Tail]) end,
    %lists:foreach(Fn, Tables),
    %ok.
