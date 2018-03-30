% ------------------------------------------------------------------------------
%
% Copyright (c) 2018, Lauri Moisio <l@arv.io>
%
% The MIT License
%
% Permission is hereby granted, free of charge, to any person obtaining a copy
% of this software and associated documentation files (the "Software"), to deal
% in the Software without restriction, including without limitation the rights
% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
% copies of the Software, and to permit persons to whom the Software is
% furnished to do so, subject to the following conditions:
%
% The above copyright notice and this permission notice shall be included in
% all copies or substantial portions of the Software.
%
% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
% THE SOFTWARE.
%
% ------------------------------------------------------------------------------
%
-module(namespace).

-export([
    compile/1,
    match/2,
    domain_hash/1,
    domain_hash/2,
    domain_trie_key/1,
    domain_trie_key/2,
    id/1,
    id_key/1
]).

-include_lib("dnslib/include/dnslib.hrl").

id('_') -> default;
id(Id) -> Id.


id_key_fun(_, Value, Map) when is_list(Value) ->
    lists:foldl(fun ({_, NsMap = #{namespace_id := Id}}, FunMap) -> FunMap#{Id => NsMap} end, Map, Value);
id_key_fun(_, Value = #{namespace_id := Id}, Map) ->
    Map#{Id => Value}.


id_key(Map) ->
    maps:fold(fun id_key_fun/3, #{}, Map).


namespace_proto_map(Base) ->
    Base#{
        handlers => #{
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
        },
        client_msg_size_refuse => 512,
        client_msg_size_drop => 512,
        recurse => false,
        max_ttl => element(2, dnslib:list_to_ttl("1 day")),
        max_negative_ttl => element(2, dnslib:list_to_ttl("1 day"))
    }.


match_netmask(_, []) ->
    undefined;
match_netmask(_, [{'_', Map}|_]) ->
    Map;
match_netmask({B1, _, _, _}, [{{{B1, _, _, _}, 8}, Map}|_]) ->
    Map;
match_netmask({B1, B2, _, _}, [{{{B1, B2, _, _}, 16}, Map}|_]) ->
    Map;
match_netmask({B1, B2, B3, _}, [{{{B1, B2, B3, _}, 24}, Map}|_]) ->
    Map;
match_netmask({B1, _, _, _, _, _, _, _}, [{{{B1, _, _, _, _, _, _, _}, 16}, Map}|_]) ->
    Map;
match_netmask({B1, B2, _, _, _, _, _, _}, [{{{B1, B2, _, _, _, _, _, _}, 32}, Map}|_]) ->
    Map;
match_netmask({B1, B2, B3, _, _, _, _, _}, [{{{B1, B2, B3, _, _, _, _, _}, 48}, Map}|_]) ->
    Map;
match_netmask({B1, B2, B3, B4, _, _, _, _}, [{{{B1, B2, B3, B4, _, _, _, _}, 64}, Map}|_]) ->
    Map;
match_netmask({B1, B2, B3, B4, B5, _, _, _}, [{{{B1, B2, B3, B4, B5, _, _, _}, 80}, Map}|_]) ->
    Map;
match_netmask({B1, B2, B3, B4, B5, B6, _, _}, [{{{B1, B2, B3, B4, B5, B6, _, _}, 96}, Map}|_]) ->
    Map;
match_netmask({B1, B2, B3, B4, B5, B6, B7, _}, [{{{B1, B2, B3, B4, B5, B6, B7, _}, 112}, Map}|_]) ->
    Map;
match_netmask(Address1, [{{Address2, Len}, Map}|Rest])
when tuple_size(Address1) =:= tuple_size(Address2) ->
    ElementBits = tuple_size(Address1) * 2, % As it happens: 4 * 2 = 8, 8 * 2 = 16
    Index = Len div ElementBits,
    LoseBits = ElementBits - (Len rem ElementBits),
    Match = element(Index+1, Address1) bsr LoseBits,
    case element(Index+1, Address2) bsr LoseBits of
        Match -> Map;
        _ -> match(Address1, Rest)
    end;
match_netmask(Address1, [_|Rest]) ->
    match(Address1, Rest).


match(Address, Map) ->
    AF = case Address of
        {_, _, _, _} -> inet;
        {_, _, _, _, _, _, _, _} -> inet6
    end,
    case Map of
        #{Address := Config} -> Config;
        #{AF := []} -> maps:get('_', Map, undefined);
        #{AF := List} ->
            case match_netmask(Address, List) of
                undefined -> maps:get('_', Map, undefined);
                Config -> Config
            end
    end.


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
    case Wildcard of
        [] -> Map1;
        [{_, WildcardConfig}] -> Map1#{'_' => WildcardConfig}
    end;
compile([Proto|Rest], Acc) ->
    compile(Rest, [compile_namespace(Proto)|Acc]).


compile_namespace({'_', Opts}) ->
    {'_', compile_opts(Opts, #{namespace_id => '_'})};
compile_namespace({{{_, _, _, _}, Length}=AddressTuple, Opts}) when Length >= 0, Length =< 32 ->
    {AddressTuple, compile_opts(Opts, #{namespace_id => AddressTuple})};
compile_namespace({{{_, _, _, _, _, _, _, _}, Length}=AddressTuple, Opts}) when Length >= 0, Length =< 128 ->
    {AddressTuple, compile_opts(Opts, #{namespace_id => AddressTuple})};
compile_namespace({{_, _, _, _} = Address, Opts}) ->
    {Address, compile_opts(Opts, #{namespace_id => Address})};
compile_namespace({{_, _, _, _, _, _, _, _} = Address, Opts}) ->
    {Address, compile_opts(Opts, #{namespace_id => Address})};
compile_namespace({[Head|_] = List, Opts0}) when not is_integer(Head) ->
    Opts1 = compile_opts(Opts0, #{namespace_id => erlang:phash2(List)}),
    lists:map(fun compile_namespace/1, [{Netmask, Opts1} || Netmask <- List]);
compile_namespace({Netmask, Opts}) when is_list(Netmask) ->
    {ok, Tuple} = kurremkarmerruk_utils:parse_netmask(Netmask),
    {Tuple, compile_opts(Opts, #{namespace_id => Tuple})}.


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
    HandlerModules = lists:flatten([HandlerList || {_, HandlerList} <- maps:to_list(Handlers)]),
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
            case Module:handle_config(Option, Value, Map0) of
                {ok, Map1} -> compile_opts_to_map(Rest, OptionMap, Map1)
            end
    end.


domain_hash(Domain) ->
    erlang:phash2(dnslib:normalize(Domain)).


domain_hash(Domain, Namespace) ->
    erlang:phash2([id(Namespace)|dnslib:normalize(Domain)]).


domain_trie_key(Domain) ->
    lists:reverse(dnslib:normalize(Domain)).


domain_trie_key(Domain, Namespace) ->
    [id(Namespace)|lists:reverse(dnslib:normalize(Domain))].
