-module(kurremkarmerruk_zone_transfer_sup).

-behavior(supervisor).
-export([init/1,start_link/0]).


init([]) ->
    {ok, {{simple_one_for_one, 10, 5}, [
        #{
            id => nil,
            %start    => {?MODULE, start_transfer, []},
            start    => {stream_transfer, start_link, []},
            type     => worker,
            restart  => temporary,
            shutdown => 1000
        }
    ]}}.


start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).


%%
%%% Internal
%%
