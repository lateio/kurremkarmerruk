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
-module(namespace).

-export([
    compile/1,
    handlers/1,
    match/2,
    match_netmask/2,
    domain_hash/1,
    domain_hash/2,
    domain_trie_key/1,
    domain_trie_key/2,
    id_key/1,
    internal/0
]).

-ifdef(EUNIT).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include_lib("dnslib/include/dnslib.hrl").


id_key_fun(_, Value, Map) when is_list(Value) ->
    lists:foldl(fun ({_, NsMap = #{namespace_id := Id}}, FunMap) -> FunMap#{Id => NsMap} end, Map, Value);
id_key_fun(_, Value = #{namespace_id := Id}, Map) ->
    Map#{Id => Value}.


id_key(Map) ->
    maps:fold(fun id_key_fun/3, #{}, Map).


namespace_proto_map(Base) ->
    Base#{
        handlers => maps:get(handlers, Base, #{
            % Or declare this by default as [{kurremkarmerruk_zone, {include_key, zone_options}}]
            % and allow zone options to be specified in a tuple with a key {zone_options, [...]}
            % This way it would be possible to have default handlers, and let user configure
            % as necessary. Other option would be to include all other namespace options and let
            % handlers figure it out.
            % Or declare this by default as [{kurremkarmerruk_zone, [include_namespace_all]]
            %
            % Though this seems prone to baffling debugging as changing handlers would also
            % by default purge them of all configuration options
            query => [kurremkarmerruk_zone, kurremkarmerruk_cache, kurremkarmerruk_recurse]
        }),
        client_msg_size_refuse => 512,
        client_msg_size_drop => 512,
        recurse => false,
        max_ttl => element(2, dnslib:list_to_ttl("1 day")),
        max_negative_ttl => element(2, dnslib:list_to_ttl("1 day"))
    }.


match_netmask(_, '_') ->
    true;
match_netmask({_, _, _, _}, {{_, _, _, _}, 0}) ->
    true;
match_netmask({B1, _, _, _}, {{B1, _, _, _}, 8}) ->
    true;
match_netmask({B1, B2, _, _}, {{B1, B2, _, _}, 16}) ->
    true;
match_netmask({B1, B2, B3, _}, {{B1, B2, B3, _}, 24}) ->
    true;
match_netmask({B1, B2, B3, B4}, {{B1, B2, B3, B4}, 32}) ->
    true;
match_netmask({_, _, _, _, _, _, _, _}, {{_, _, _, _, _, _, _, _}, 0}) ->
    true;
match_netmask({B1, _, _, _, _, _, _, _}, {{B1, _, _, _, _, _, _, _}, 16}) ->
    true;
match_netmask({B1, B2, _, _, _, _, _, _}, {{B1, B2, _, _, _, _, _, _}, 32}) ->
    true;
match_netmask({B1, B2, B3, _, _, _, _, _}, {{B1, B2, B3, _, _, _, _, _}, 48}) ->
    true;
match_netmask({B1, B2, B3, B4, _, _, _, _}, {{B1, B2, B3, B4, _, _, _, _}, 64}) ->
    true;
match_netmask({B1, B2, B3, B4, B5, _, _, _}, {{B1, B2, B3, B4, B5, _, _, _}, 80}) ->
    true;
match_netmask({B1, B2, B3, B4, B5, B6, _, _}, {{B1, B2, B3, B4, B5, B6, _, _}, 96}) ->
    true;
match_netmask({B1, B2, B3, B4, B5, B6, B7, _}, {{B1, B2, B3, B4, B5, B6, B7, _}, 112}) ->
    true;
match_netmask({B1, B2, B3, B4, B5, B6, B7, B8}, {{B1, B2, B3, B4, B5, B6, B7, B8}, 128}) ->
    true;
match_netmask(Address1, {Address2, Len})
when tuple_size(Address1) =:= tuple_size(Address2), Len rem (tuple_size(Address1) * 2) > 0 ->
    ElementBits = tuple_size(Address1) * 2, % As it happens: 4 * 2 = 8, 8 * 2 = 16
    Index = Len div ElementBits,
    LoseBits = ElementBits - (Len rem ElementBits),
    Match = element(Index+1, Address1) bsr LoseBits,
    Match =:= element(Index+1, Address2) bsr LoseBits;
match_netmask(_, _) ->
    false.

-ifdef(EUNIT).
match_netmask_test() ->
    true = match_netmask({0,0,0,0}, {{0,0,0,0}, 0}),
    true = lists:all(fun (Netmask) -> true = match_netmask({16#FF,0,0,0}, {{16#FE,0,0,0}, Netmask}) end, lists:seq(1,7)),
    false = match_netmask({16#FF,0,0,0}, {{16#FE,0,0,0}, 8}),
    false = match_netmask({16#FF,0,0,0}, {{16#EF,0,0,0}, 8}),
    true = match_netmask({0,0,0,0,0,0,0,0}, {{0,0,0,0,0,0,0,0}, 0}),
    true = lists:all(fun (Netmask) -> true = match_netmask({16#FFFF,0,0,0,0,0,0,0}, {{16#FFFE,0,0,0,0,0,0,0}, Netmask}) end, lists:seq(1,15)),
    false = match_netmask({16#FFFF,0,0,0,0,0,0,0}, {{16#FFFE,0,0,0,0,0,0,0}, 16}),
    true = match_netmask({16#FFFF,0,0,0,0,0,0,0}, {{16#FFFE,0,0,0,0,0,0,0}, 8}),
    false = match_netmask({16#FEFF,0,0,0,0,0,0,0}, {{16#FFFF,0,0,0,0,0,0,0}, 16}),
    true = lists:all(fun (Netmask) -> true = match_netmask({16#FFFF,16#FFFF,0,0,0,0,0,0}, {{16#FFFF,16#FFFE,0,0,0,0,0,0}, Netmask}) end, lists:seq(1,31)),
    false = match_netmask({16#FFFF,16#FFFF,0,0,0,0,0,0}, {{16#FFFF,16#FFFE,0,0,0,0,0,0}, 32}),
    false = match_netmask({16#FFFE,16#FFFF,0,0,0,0,0,0}, {{16#FFFF,16#FFFF,0,0,0,0,0,0}, 32}),
    true = match_netmask({16#FFFE,16#FFFF,0,0,0,0,0,0}, {{16#FFFF,16#FFFF,0,0,0,0,0,0}, 24}).
-endif.


match_netmask_fn(Address) ->
    fun ({Netmask, _}) -> not match_netmask(Address, Netmask) end.

match(Address, Map) ->
    AF = case Address of
        {_, _, _, _} -> inet;
        {_, _, _, _, _, _, _, _} -> inet6
    end,
    case Map of
        %#{Address := Config} -> Config;
        #{AF := []} -> maps:get('_', Map, undefined);
        #{AF := List} ->
            case lists:splitwith(match_netmask_fn(Address), List) of
                {_, [{_, Config}|_]} -> Config;
                {_, []} -> maps:get('_', Map, undefined)
            end
    end.


-spec compile(term()) -> {'ok', Namespaces :: map()} | 'abort'.
compile(Config) ->
    compile(proplists:get_value(config, Config, []), []).


compile([], Acc0) ->
    Acc1 = lists:flatten(Acc0),
    {Addresses, Acc2} = lists:partition(fun ({Addr, _}) when is_tuple(Addr) -> tuple_size(Addr) =/= 2; (_) -> false end, Acc1),
    {IPV4_0, Acc3} = lists:partition(fun ({{Addr4, _}, _}) -> tuple_size(Addr4) =:= 4; (_) -> false end, Acc2),
    {IPV6_0, Wildcard} = lists:partition(fun ({{Addr6, _}, _}) -> tuple_size(Addr6) =:= 8; (_) -> false end, Acc3),
    Map0 = maps:from_list(Addresses),
    ToWildcard = fun ({{_, 0}, Config}) -> {'_', Config}; (Tuple) -> Tuple end,
    IPV4_1 = lists:map(ToWildcard, IPV4_0),
    IPV6_1 = lists:map(ToWildcard, IPV6_0),
    UniqueFn = fun
        (_, false) -> false;
        ({Key, _}, [_|Rest]) ->
            case lists:keymember(Key, 1, Rest) of
                true -> false;
                false -> Rest
            end
    end,
    [] = lists:foldl(UniqueFn, IPV4_1, IPV4_1),
    [] = lists:foldl(UniqueFn, IPV6_1, IPV6_1),
    SortFn = fun
        ({'_', _}, _) -> false;
        ({{_, Len1}, _}, {{_, Len2}, _}) -> Len1 >= Len2
    end,
    IPV4_2 = lists:sort(SortFn, IPV4_0),
    IPV6_2 = lists:sort(SortFn, IPV6_0),
    % Make sure that both IPV4_1 and IPV6_1 only contain unique entries.
    Map1 = Map0#{
        inet => IPV4_2,
        inet6 => IPV6_2
    },
    {ok,
        case Wildcard of
            [] -> Map1;
            [{_, WildcardConfig}] -> Map1#{'_' => WildcardConfig}
        end
    };
compile([Proto|Rest], Acc) ->
    try compile_namespace(Proto) of
        Namespace -> compile(Rest, [Namespace|Acc])
    catch
        abort -> abort
    end.


compile_namespace(Term) ->
    compile_namespace(Term, fun erlang:phash2/1).

compile_namespace({'_', Opts}, Fn) ->
    {'_', compile_opts(Opts, #{namespace_id => Fn('_')})};
compile_namespace({{{_, _, _, _}, Length}=AddressTuple, Opts}, Fn) when Length >= 0, Length =< 32 ->
    {AddressTuple, compile_opts(Opts, #{namespace_id => Fn(AddressTuple)})};
compile_namespace({{{_, _, _, _, _, _, _, _}, Length}=AddressTuple, Opts}, Fn) when Length >= 0, Length =< 128 ->
    {AddressTuple, compile_opts(Opts, #{namespace_id => Fn(AddressTuple)})};
compile_namespace({{_, _, _, _} = Address, Opts}, Fn) ->
    {Address, compile_opts(Opts, #{namespace_id => Fn(Address)})};
compile_namespace({{_, _, _, _, _, _, _, _} = Address, Opts}, Fn) ->
    {Address, compile_opts(Opts, #{namespace_id => Fn(Address)})};
compile_namespace({[Head|_] = List, Opts0}, Fn) when not is_integer(Head) ->
    Fn2 = fun (FunId) when FunId =/= '_' -> erlang:phash2(FunId) end,
    WrapFn = fun (FunTerm) -> compile_namespace(FunTerm, Fn2) end,
    Opts1 = compile_opts(Opts0, #{namespace_id => Fn(lists:sort(List))}),
    lists:map(WrapFn, [{Netmask, Opts1} || Netmask <- List]);
compile_namespace({[Head|_] = Netmask, Opts}, Fn) when is_integer(Head) ->
    {ok, Tuple} = kurremkarmerruk_utils:parse_netmask(Netmask),
    {Tuple, compile_opts(Opts, #{namespace_id => Fn(Tuple)})};
compile_namespace({{Nick, Spec}, Config}, _) when is_atom(Nick), Nick =/= '__Internal__' ->
    compile_namespace({Spec, Config}, fun (_) -> Nick end).


compile_opts(Map, _) when is_map(Map) ->
    Map;
compile_opts({priv_file, App, Path}, Base) ->
    {ok, Opts} = file:consult(filename:join(code:priv_dir(App), Path)),
    compile_opts(Opts, Base);
compile_opts({file, Path}, Base) ->
    {ok, Opts} = file:consult(Path),
    compile_opts(Opts, Base);
compile_opts(Opts, Base) ->
    % Allow all known (how do we know about them?) handlers to participate in
    % Default map construction? Do we look through the list once to find all
    % unique handlers?
    {Default, OptionMap} = default_map_n_handlers(Opts, namespace_proto_map(Base)),
    compile_opts_to_map(Opts, OptionMap, Default).


default_map_n_handlers([], Map0 = #{handlers := Handlers}) ->
    % Allow handlers to create processes under a supervisor? We'd need to supply the
    % Pid for the supervisor
    HandlerModules = lists:append([HandlerList || {_, HandlerList} <- maps:to_list(Handlers)]),
    Map1 = lists:foldl(fun (Module, FunMap) -> Module:config_init(FunMap) end, Map0, HandlerModules),
    OptionMap = lists:foldl(
        fun (Module, TmpOptionMap) ->
            lists:foldl(fun (Option, InnerOptionMap) -> InnerOptionMap#{Option => Module} end, TmpOptionMap, Module:config_keys())
        end,
    #{}, HandlerModules),
    % Prepare a map of option => module
    {Map1, OptionMap};
default_map_n_handlers([{handlers, {Opcode, OpcodeHandlers}}|Rest], Map = #{handlers := Handlers}) ->
    true = dnslib:is_valid_opcode(Opcode),
    default_map_n_handlers(Rest, Map#{handlers => Handlers#{Opcode => OpcodeHandlers}});
default_map_n_handlers([_|Rest], Map) ->
    default_map_n_handlers(Rest, Map).


compile_opts_to_map([], _, Map = #{handlers := Handlers}) ->
    % Allow handlers to check namespace map and decide on if they should be included?
    % Also, let them set flags/values? Like resolve, cache, etc.
    HandlerModules = lists:flatten([HandlerList || {_, HandlerList} <- maps:to_list(Handlers)]),
    lists:foldl(fun (Module, FunMap) -> Module:config_end(FunMap) end, Map, HandlerModules);
compile_opts_to_map([{handlers, _}|Rest], OptionMap, Map) ->
    compile_opts_to_map(Rest, OptionMap, Map);
compile_opts_to_map([{client_msg_size_refuse, Size}|Rest], OptionMap, Map) ->
    case Size of
        _ when Size > 0, Size < 16#FFFF -> compile_opts_to_map(Rest, OptionMap, Map#{client_msg_size_refuse => Size})
    end;
compile_opts_to_map([{client_msg_size_drop, Size}|Rest], OptionMap, Map) ->
    case Size of
        _ when Size > 0, Size < 16#FFFF -> compile_opts_to_map(Rest, OptionMap, Map#{client_msg_size_drop => Size})
    end;
compile_opts_to_map([{Opt, Config}|Rest], OptionMap, Map) when Opt =:= max_ttl; Opt =:= max_negative_ttl ->
    case Config of
        _ when Config >= 0, Config =< ?MAX_TTL -> compile_opts_to_map(Rest, OptionMap, Map#{Opt => Config});
        _ when is_list(Config) ->
            {ok, Ttl} = dnslib:list_to_ttl(Config),
            compile_opts_to_map(Rest, OptionMap, Map#{Opt => Ttl})
    end;
compile_opts_to_map([{Option, Value}|Rest], OptionMap, Map0) ->
    case maps:get(Option, OptionMap, undefined) of
        undefined -> %error({unknown_option, Option});
            compile_opts_to_map(Rest, OptionMap, Map0);
        Module ->
            try Module:handle_config(Option, Value, Map0) of
                {ok, Map1} -> compile_opts_to_map(Rest, OptionMap, Map1);
                abort -> throw(abort)
            catch
                abort -> throw(abort)
            end
    end.


domain_hash(Domain) ->
    erlang:phash2(dnslib:normalize_domain(Domain)).

domain_hash(Domain, Prefix) when is_list(Prefix) ->
    erlang:phash2(lists:append(Prefix, dnslib:normalize_domain(Domain)));
domain_hash(Domain, Namespace) ->
    domain_hash(Domain, [Namespace]).


domain_trie_key(Domain) ->
    lists:reverse(dnslib:normalize_domain(Domain)).

domain_trie_key(Domain, Prefix) when is_list(Prefix) ->
    lists:append(Prefix, lists:reverse(dnslib:normalize_domain(Domain)));
domain_trie_key(Domain, Namespace) ->
    domain_trie_key(Domain, [Namespace]).


handlers(Config) ->
    handlers(proplists:get_value(config, Config, []), []).

handlers([], Handlers) ->
    lists:usort(lists:append(Handlers));
handlers([{_, Namespaconfig}|Rest], Handlers) ->
    case proplists:get_value(handlers, Namespaconfig) of
        {_, Ophandlers} -> handlers(Rest, [Ophandlers|Handlers]);
        undefined ->
            handlers(Rest, [
                [
                    kurremkarmerruk_zone,
                    kurremkarmerruk_cache,
                    kurremkarmerruk_recurse
                ]|Handlers]
            )
    end.


internal() ->
    {ok, Map0} = application:get_env(kurremkarmerruk, namespace_internal_default),
    Map1 = Map0#{opcode => '_'},
    % How to allow kurremkarmerruk_zone to be used in internal namespace?
    case kurremkarmerruk_handler:get_handlers('_', Map1) of
        Handlers when Handlers =/= refused ->
            Map2 = lists:foldl(fun (FunMod, FunMap) -> FunMod:config_init(FunMap) end, Map1, Handlers),
            _Options = maps:from_list(lists:foldl(fun (FunMod, OptList) -> [{FunMod, GenOpt} || GenOpt <- FunMod:config_keys() ] ++ OptList end, [], Handlers)),
            Opts = [
                {recurse, true},
                {cache, true}
            ],
            Map3 = compile_opts(Opts, Map2),
            lists:foldl(fun (FunMod, FunMap) -> FunMod:config_end(FunMap) end, Map3, Handlers)
    end.
