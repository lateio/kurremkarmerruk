-module(kurremkarmerruk_zone_sup).

-behavior(supervisor).
-export([init/1,start_link/0]).


init([]) ->
    {ok, {{one_for_one, 2, 2}, [
        #{
            id => kurremkarmerruk_zone_server,
            start => {kurremkarmerruk_zone_server, start_link, []},
            type => worker,
            restart => permanent,
            shutdown => 2000
        },
        #{
            id => kurremkarmerruk_zone_transfer_sup,
            start => {kurremkarmerruk_zone_transfer_sup, start_link, []},
            type => supervisor,
            restart => permanent,
            shutdown => 5000
        }
    ]}}.


start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).
