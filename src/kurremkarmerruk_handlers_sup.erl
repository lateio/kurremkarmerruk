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
%%%-------------------------------------------------------------------
%% @doc kurremkarmerruk top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(kurremkarmerruk_handlers_sup).

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
    Handlers = namespace:handlers([{config, [{nil, []}]}]),
    CacheRec = [
        #{
            id => kurremkarmerruk_cache_sup,
            start => {kurremkarmerruk_cache_sup, start_link, []},
            type => supervisor,
            restart => permanent,
            shutdown => 5000
        },
        #{
            id => kurremkarmerruk_recurse_sup,
            start => {kurremkarmerruk_recurse_sup, start_link, []},
            type => supervisor,
            restart => permanent,
            shutdown => 5000
        }
    ],
    {ok, {{one_for_one, 2, 2}, CacheRec ++ lists:foldr(fun childspec_fun/2, [], Handlers)}}.

%%====================================================================
%% Internal functions
%%====================================================================

childspec_fun(Module, Acc) ->
    case Module:spawn_handler_proc() of
        false -> Acc;
        true ->
            [#{
                id       => Module,
                start    => {Module, start_link, []},
                type     => worker,
                restart  => permanent,
                shutdown => 2000
            }|Acc];
        {M,F,A} ->
            [#{
                id       => M,
                start    => {M, F, A},
                type     => worker,
                restart  => permanent,
                shutdown => 2000
            }|Acc];
        {M,F,A,Opts} ->
            [#{
                id       => maps:get(id, Opts, M),
                start    => {M, F, A},
                type     => maps:get(type, Opts, worker),
                restart  => maps:get(restart, Opts, permanent),
                shutdown => maps:get(shutdown, Opts, 2000)
            }|Acc]
    end.
