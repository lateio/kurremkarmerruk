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
-module(kurremkarmerruk_server).

-behavior(gen_server).
-export([init/1,code_change/3,terminate/2,handle_call/3,handle_cast/2,handle_info/2]).

-export([
    start_link/0,
    namespaces/0,
    namespaces_id_keyed/0,
    config/0
]).

-record(state, {namespaces,namespaces_by_id,config}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


namespaces() ->
    gen_server:call(?MODULE, namespaces).


namespaces_id_keyed() ->
    gen_server:call(?MODULE, namespaces_id_keyed).


config() ->
    gen_server:call(?MODULE, config).


init(_) ->
    process_flag(trap_exit, true),
    ets:new(client_requests, [named_table, public]),
    Config0 = application:get_all_env(kurremkarmerruk),
    Config = case proplists:get_value(config_file, Config0) of
        undefined -> Config0;
        {priv_file, App, Path} ->
            {ok, CaseConfig} = file:consult(filename:join(code:priv_dir(App), Path)),
            CaseConfig;
        {file, Path} ->
            {ok, CaseConfig} = file:consult(Path),
            CaseConfig
    end,
    case proplists:get_value(data_dir, Config) of
        undefined -> ok;
        DataDir ->
            case filelib:is_dir(DataDir) of
                true -> application:set_env(kurremkarmerruk, data_dir, DataDir);
                false -> skip
        end
    end,
    case namespace:compile(Config) of
        {ok, Namespaces} ->
            % Complain about unreachable namespaces...
            {ok, #state{
                namespaces=Namespaces,
                namespaces_by_id=namespace:id_key(Namespaces),
                config=Config
            }};
        abort ->
            init:stop(1),
            {ok, nil}
    end.


code_change(_, State, _) ->
    {ok, State}.


terminate(_, #state{}) ->
    ok.


handle_call(namespaces, _, State = #state{namespaces=Namespaces}) ->
    {reply, Namespaces, State};
handle_call(namespaces_id_keyed, _, State = #state{namespaces_by_id=Namespaces}) ->
    {reply, Namespaces, State};
handle_call(config, _, State = #state{config=Config}) ->
    {reply, Config, State};
handle_call(_, _, State) ->
    {noreply, State}.


handle_cast(_, State) ->
    {noreply, State}.


handle_info(_, State) ->
    {noreply, State}.
