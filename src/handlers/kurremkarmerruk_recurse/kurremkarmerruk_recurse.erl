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
-module(kurremkarmerruk_recurse).

-behavior(kurremkarmerruk_handler).
-export([execute/2,valid_opcodes/0,config_keys/0,config_init/1,handle_config/3,config_end/1,spawn_handler_proc/0]).

-export([
    resolve/1,
    resolve/2
]).

-include_lib("kurremkarmerruk/include/recurse.hrl").

-define(SIN_LIMIT, 20).


spawn_handler_proc() -> false.
valid_opcodes() -> query.
config_keys() -> [recurse].


config_init(Map) ->
    Map#{
        recurse_config => #{
            servers => [],
            recurse_strategy => ?DEFAULT_STRATEGY,
            recurse_unknown_types => false,
            recurse_unknown_classes => false,
            recurse_disallow_types => [all, maila, mailb],
            recurse_disallow_classes => [any, none]
        }
    }.


config_end(Map = #{recurse_config := Config}) ->
    case Config of
        #{servers := true} -> Map#{recurse => true, recurse_config => Config#{recurse_strategy => ?ITERATE_STRATEGY}};
        #{servers := false} -> maps:remove(recurse_config, Map#{recurse => false});
        #{servers := []} -> maps:remove(recurse_config, Map#{recurse => false});
        _ -> Map#{recurse => true}
    end.


handle_config(recurse, Config, Map = #{recurse_config := RecurseConfig0}) ->
    RecurseConfig1 = case Config of
        false -> RecurseConfig0#{servers => false};
        true  -> RecurseConfig0#{servers => true};
        [Head|_] = Host when is_integer(Head) -> RecurseConfig0#{servers => [handle_recurse(Host)]};
        Specs when is_list(Specs) ->
            % In this list might be any of the possible forms
            List = lists:flatten(lists:map(fun handle_recurse/1, Specs)),
            RecurseConfig0#{servers => List};
        {[Head|_], _} = Tuple when is_integer(Head) -> RecurseConfig0#{servers => [handle_recurse(Tuple)]};
        {Hosts, Opts} when is_list(Hosts) ->
            Specs = [{Host, Opts} || Host <- Hosts],
            List = lists:flatten(lists:map(fun handle_recurse/1, Specs)),
            RecurseConfig0#{servers => List}
    end,
    {ok, Map#{recurse_config => RecurseConfig1}}.


handle_recurse(Host) when is_list(Host) ->
    % Parameter is "dns-server.com"
    case kurremkarmerruk_utils:parse_host_port(Host, 53) of
        {ok, Spec} -> handle_recurse_final_form(Spec, [])
    end;
handle_recurse({[Head|_] = Host, Opts}) when is_integer(Head), is_list(Opts) ->
    % Parameter is {"dns-server.com", []}
    % Also, port might be replaced in opts.
    case kurremkarmerruk_utils:parse_host_port(Host, default) of
        {ok, Spec} -> handle_recurse_final_form(Spec, Opts)
    end;
handle_recurse({Hosts, Opts}) when is_list(Hosts), is_list(Opts) ->
    % Parameter is {["dns-server.com", "localhost"], []}
    Tls = lists:member(tls, Opts),
    Basic = lists:member(basic, Opts),
    Multiproto = Tls andalso Basic,
    Fn = case Multiproto of
        true ->
            fun
                (Host) when is_list(Host) ->
                    {ok, Tuple} = kurremkarmerruk_utils:parse_host_port(Host, default),
                    Tuple;
                (Address) when is_tuple(Address), tuple_size(Address) =/= 2 -> {Address, default}
            end;
        false ->
            fun
                (Host) when is_list(Host) ->
                    {ok, Tuple} = kurremkarmerruk_utils:parse_host_port(Host, default),
                    Tuple;
                (Address) when is_tuple(Address), tuple_size(Address) =/= 2 -> {Address, default};
                ({_, _}=Tuple) -> Tuple
            end
    end,
    lists:flatten([handle_recurse_final_form(Spec, Opts) || Spec <- lists:map(Fn, Hosts)]);
handle_recurse({Address, Port}=Tuple) when is_tuple(Address), Port > 0, Port =< 16#FFFF ->
    handle_recurse_final_form(Tuple, []);
handle_recurse({{Address, Port}=Tuple, Opts}) when is_tuple(Address), is_list(Opts), Port > 0, Port =< 16#FFFF ->
    handle_recurse_final_form(Tuple, Opts).


handle_recurse_final_form({Address, Port}, Opts0) ->
    Tls = lists:member(tls, Opts0),
    Basic = lists:member(basic, Opts0) orelse not Tls,
    Opts1 = [GenOpt || GenOpt <- Opts0, GenOpt =/= basic, GenOpt =/= tls],
    Fn = fun
        (_, FunPort) when is_integer(FunPort) -> FunPort;
        (tls, default) -> 853;
        (basic, default) -> 53
    end,
    MapList = [Proto || {Proto, _}  <- lists:filter(fun ({_, Enabled}) -> Enabled end, [{tls, Tls}, {basic, Basic}])],
    case lists:map(fun (Proto) -> {{Proto, {Address, Fn(Proto, Port)}}, Opts1} end, MapList) of
        [Tuple] -> Tuple;
        RetList -> RetList
    end.


execute(Msg = #{'Recursion_desired' := false}, _) ->
    {ok, Msg};
execute(Msg, Namespace = #{recurse_config := _}) ->
    {ok, Answers} = kurremkarmerruk_recurse_resolve:resolve(recurse_questions(Msg), [{namespace, Namespace}]),
    cache_results(Namespace, Answers),
    {ok, kurremkarmerruk_handler:add_answers(Answers, false, Msg)};
execute(Msg, _) ->
    {ok, Msg}.


cache_results(Namespace, Results) ->
    % Since recurse will dip into zone and cache, some of the results will be
    % sourced locally and thus require no caching. Need to keep track of that
    case kurremkarmerruk_cache:namespace_cache_enabled(Namespace) of
        true -> kurremkarmerruk_cache:store_interpret_results(Namespace, Results);
        false -> skip
    end.


recurse_questions(Msg = #{questions := Questions}) ->
    recurse_questions(Questions, Msg, []).

recurse_questions([], _, Acc) ->
    Acc;
recurse_questions([Question|Rest], Msg, Acc) ->
    case kurremkarmerruk_handler:current_answers(Question, Msg) of
        [] -> recurse_questions(Rest, Msg, [Question|Acc]);
        Answers ->
            case [GenTuple || GenTuple <- Answers, element(2, GenTuple) =:= referral orelse element(2, GenTuple) =:= addressless_referral] of
                [] -> recurse_questions(Rest, Msg, [Question|Acc]);
                %[Referral] -> recurse_questions(Rest, Msg, [Referral|Acc])
                % Because zone and cache might both return the same referral (because cache consults zone)
                % the message might contain multiple referrals... Should prune answers based on their quality/closeness?
                Referrals -> recurse_questions(Rest, Msg, [hd(Referrals)|Acc])
            end
    end.


% Don't bleed messages into the process...
resolve(Query) ->
    kurremkarmerruk_recurse_resolve:resolve(Query).

resolve(Query, Opts) ->
    kurremkarmerruk_recurse_resolve:resolve(Query, Opts).
