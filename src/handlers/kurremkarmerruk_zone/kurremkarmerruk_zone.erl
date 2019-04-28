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
-module(kurremkarmerruk_zone).
-behavior(kurremkarmerruk_handler).
-export([execute/2,valid_opcodes/0,config_keys/0,config_init/1,handle_config/3,config_end/1,spawn_handler_proc/0]).

-export([
    execute_query/2,
    authoritative_for/2,
    namespace_zone_enabled/1,
    override_authoritative/3
    % Should have a command to force refresh of all zones...
]).

-include_lib("kernel/include/logger.hrl").
-include_lib("dnslib/include/dnslib.hrl").

spawn_handler_proc() -> {kurremkarmerruk_zone_sup, start_link, [], #{type => supervisor, shutdown => 5000}}.
valid_opcodes() -> query.
config_keys() -> [zone,refuse_types].


config_init(Map) ->
    Map#{
        zone_config => #{
            module => kurremkarmerruk_zone_server:storage_module(),
            fulfill_queries => true,
            allow_zone_transfer => [] % Allow zone transfer (List of apex domains?). Allow only certain classes? Or by default allow all, but permit a form which only allows a certain class
        }
    }.


config_end(Map) ->
    % Should we check that any and all zones which are allowed to be transferred have sane SOAs?
    Map.


handle_config(zone, ZoneSpec, Namespace = #{zone_config := ZoneConfig, namespace_id := NSID}) ->
    case
        case ZoneSpec of
            {priv_file, App, TmpPath} -> {file, filename:join(code:priv_dir(App), TmpPath), []};
            {priv_file, App, TmpPath, TmpOpts} -> {file, filename:join(code:priv_dir(App), TmpPath), TmpOpts};
            {file, TmpPath} -> {file, TmpPath, []};
            {file, TmpPath, TmpOpts} -> {file, TmpPath, TmpOpts};
            {priv_dir, App, TmpPath} -> {dir, filename:join(code:priv_dir(App), TmpPath), []};
            {priv_dir, App, TmpPath, TmpOpts} -> {dir, filename:join(code:priv_dir(App), TmpPath), TmpOpts};
            {dir, TmpPath} -> {dir, TmpPath, []};
            {dir, TmpPath, TmpOpts} -> {dir, TmpPath, TmpOpts};
            {zone_transfer, TmpApex} -> {zone_transfer, TmpApex, in, [], []};
            {zone_transfer, TmpApex, TmpClass} -> {zone_transfer, TmpApex, TmpClass, [], []};
            {zone_transfer, TmpApex, TmpClass, TmpOpts} -> {zone_transfer, TmpApex, TmpClass, [], TmpOpts};
            {zone_transfer, TmpApex, TmpClass, TmpOpts, TmpMaster} -> {zone_transfer, TmpApex, TmpClass, TmpMaster, TmpOpts};
            %{git, URI, Opts} ... Like rebar3... And then, like transfer, use SOA refresh to pull from the source.
            {RrList, RrOpts} when is_list(RrList), is_list(RrOpts) -> {list, RrList, RrOpts};
            RrList when is_list(RrList) -> {list, RrList, []}
        end
    of
        % Handling zone collisions
        % Make sure that watch_ignore_serial is not set when zone_transfers are allowed...
        {file, Path, Opts0} ->
            {Opts, ConsultOpts} = lists:partition(fun general_zone_opt/1, Opts0),
            case kurremkarmerruk_zone_server:new_zone_file(Path, ConsultOpts, NSID) of
                {ok, Soa} -> {ok, Namespace#{zone_config => handle_general_zone_opts([Soa], Opts, ZoneConfig)}};
                {error, Reason} ->
                    ?LOG_ERROR("Loading zone from file ~s failed: ~p", [Path, Reason]),
                    abort
            end;
        {dir, Path, Opts0} ->
            {Opts, DirOpts} = lists:partition(fun general_zone_opt/1, Opts0),
            case kurremkarmerruk_zone_server:new_zone_dir(Path, DirOpts, NSID) of
                {ok, Soas} -> {ok, Namespace#{zone_config => handle_general_zone_opts(Soas, Opts, ZoneConfig)}};
                {error, Reason} ->
                    ?LOG_ERROR("Loading zones from dir ~s failed: ~p", [Path, Reason]),
                    abort
            end;
        {zone_transfer, Apex0, Class0, Primaries0, Opts0} ->
            {Opts, _TransferOpts} = lists:partition(fun general_zone_opt/1, Opts0),
            Apex =
                try dnslib:domain(Apex0)
                catch
                    error:badarg ->
                        ?LOG_ERROR("Loading zone via zone transfer failed: Invalid domain ~p", [Apex0]),
                        throw(abort)
                end,
            Class =
                try dnslib:class(Class0)
                catch
                    error:badarg ->
                        ?LOG_ERROR("Loading zone via zone transfer failed: Invalid class ~p", [Class0]),
                        throw(abort)
                end,
            Primaries = case Primaries0 of
                [] -> % Nothing provided, use the primary nameserver of the zone
                    {ok, [{_, ok, [SoaRR]}]} = kurremkarmerruk_recurse:resolve(dnslib:question(Apex, soa, Class)),
                    [{dnsrr_soa:nameserver(SoaRR), 53, []}];
                _ when is_list(Primaries0), is_integer(hd(Primaries0)) -> [verify_transfer_primary(Primaries0)]; % Primary is a string of some sort
                _ when is_list(Primaries0) -> [verify_transfer_primary(GenPrimary) || GenPrimary <- Primaries0];
                _ when is_tuple(Primaries0) -> [verify_transfer_primary(Primaries0)]
            end,
            case kurremkarmerruk_zone_server:new_zone_transfer(Apex, Class, Primaries, Opts, NSID) of
                ok -> {ok, Namespace#{zone_config => handle_general_zone_opts({Apex, Class}, Opts, ZoneConfig)}};
                {error, Reason} ->
                    ?LOG_ERROR("Loading zone ~s ~p via zone transfer failed: ~p", [lists:droplast(dnslib:domain_to_list(Apex)), Class, Reason]),
                    abort
            end;
        {list, Rrs0, Opts} ->
            Rrs = lists:map(fun list_to_resource_term/1, Rrs0),
            case kurremkarmerruk_zone_server:new_zone_terms(Rrs, NSID) of
                {ok, Soa} -> {ok, Namespace#{zone_config => handle_general_zone_opts([Soa], Opts, ZoneConfig)}};
                {error, Reason} ->
                    ?LOG_ERROR("Loading zone from terms failed: ~p", [Reason]),
                    abort
            end
    end;
handle_config(allow_zone_transfer, true, Namespace = #{zone_config := ZoneConfig}) ->
    {ok, Namespace#{zone_config => ZoneConfig#{allow_zone_transfer => '_'}}}.


general_zone_opt(allow_zone_transfer) -> true;
general_zone_opt({allow_zone_transfer, _}) -> true;
general_zone_opt(_) -> false.

verify_transfer_primary(List) when is_list(List) ->
    case kurremkarmerruk_utils:parse_host_port(List, 53) of
        {ok, {Address, Port}} -> {Address, Port, []};
        _ ->
            ?LOG_ERROR("Loading zone via zone transfer failed: Invalid primary spec ~p", [List]),
            throw(abort)
    end;
verify_transfer_primary(Tuple) when is_tuple(Tuple) ->
    {Address, Port, Opts} = case tuple_size(Tuple) of
        3 -> Tuple;
        2 -> erlang:append_element(Tuple, []);
        _ -> {Tuple, 53, []}
    end,
    VerifyFn = case tuple_size(Address) of
        4 -> fun (TupleMember) -> is_integer(TupleMember) andalso TupleMember >= 0 andalso TupleMember =< 16#FF end;
        8 -> fun (TupleMember) -> is_integer(TupleMember) andalso TupleMember >= 0 andalso TupleMember =< 16#FFFF end;
        _ ->
            ?LOG_ERROR("Loading zone via zone transfer failed: Invalid primary spec ~p", [Tuple]),
            throw(abort)
    end,
    % Should also verify opts...
    case is_tuple(Address) andalso
         lists:all(VerifyFn, tuple_to_list(Address)) andalso
         is_integer(Port) andalso
         Port >= 0 andalso
         Port =< 16#FFFF
    of
        true -> {Address, Port, Opts};
        false ->
            ?LOG_ERROR("Loading zone via zone transfer failed: Invalid primary address ~p", [Tuple]),
            throw(abort)
    end.


list_to_resource_term(List) when is_list(List) ->
    try dnslib:resource(List)
    catch
        error:badarg ->
            ?LOG_ERROR("Invalid resource ~p", [List]),
            throw(abort)
    end;
list_to_resource_term({Do, T, C, TTL, Da}=Tuple)  ->
    try dnslib:resource(Do, T, C, TTL, Da)
    catch
        error:badarg ->
            ?LOG_ERROR("Invalid resource ~p", [Tuple]),
            throw(abort)
    end;
list_to_resource_term(Term)  ->
    ?LOG_ERROR("Invalid resource term ~p", [Term]),
    throw(abort).


handle_general_zone_opts([], _, ZoneConfig) ->
    ZoneConfig;
handle_general_zone_opts([Soa|Rest], Opts, ZoneConfig) ->
    handle_general_zone_opts(Rest, Opts, handle_general_zone_opts({?RESOURCE_DOMAIN(Soa), ?RESOURCE_CLASS(Soa)}, Opts, ZoneConfig));
handle_general_zone_opts({_, _}, [], ZoneConfig) ->
    ZoneConfig;
handle_general_zone_opts({Apex, Class}=ZoneTuple, [allow_zone_transfer|Rest], ZoneConfig = #{allow_zone_transfer := AllowList}) ->
    % Check if zone_config already allows the zone to be transferred,
    handle_general_zone_opts(ZoneTuple, Rest, ZoneConfig#{allow_zone_transfer => [{dnslib:normalize_domain(Apex), Class}|AllowList]});
handle_general_zone_opts({Apex, Class}=ZoneTuple, [{allow_zone_transfer, Opts}|Rest], ZoneConfig = #{allow_zone_transfer := AllowList}) ->
    % Check if zone_config already allows the zone to be transferred,
    Iplist = lists:map(fun (FunStr) -> {ok, Ip} = kurremkarmerruk_utils:parse_netmask(FunStr), Ip end, Opts),
    handle_general_zone_opts(ZoneTuple, Rest, ZoneConfig#{allow_zone_transfer => [{dnslib:normalize_domain(Apex), Class, [{ip, Iplist}]}|AllowList]}).


execute(#{'Opcode' := query} = Message, Namespace) ->
    case is_zone_transfer(Message) of
        false -> kurremkarmerruk_zone_query:execute(Message, Namespace);
        format_error -> {stop, dnsmsg:set_response_header(Message, return_code, format_error)};
        _  -> kurremkarmerruk_zone_transfer:execute(Message, Namespace)
    end;
execute(Message, _) ->
    {ok, Message}.


is_zone_transfer(#{'Questions' := [{_, axfr, _}]}) -> axfr;
is_zone_transfer(#{'Questions' := [{_, ixfr, _}]}) -> ixfr;
is_zone_transfer(#{'Questions' := Questions}) ->
    case [GenQ || GenQ <- Questions, ?QUESTION_TYPE(GenQ) =:= axfr orelse ?QUESTION_TYPE(GenQ) =:= ixfr] of
        [] -> false;
        _ -> format_error
    end.


execute_query(Query, Namespace) ->
    kurremkarmerruk_zone_query:execute_query(Query, Namespace).


namespace_zone_enabled(Namespace) ->
    namespace_enabled(Namespace).


namespace_enabled(#{zone_config := _}) ->
    true;
namespace_enabled(_) ->
    false.


override_authoritative([], _, Acc) ->
    Acc;
override_authoritative([Tuple|Rest], Namespace, Acc) when element(2, element(1, Tuple)) =:= cname ->
    override_authoritative(Rest, Namespace, [Tuple|Acc]);
override_authoritative([Tuple|Rest], Namespace, Acc) ->
    case Tuple of
        % What about referral, cname_loop, cname_referral?
        {_, ok, Resources} -> Resources;
        {_, cname, {_, Resources}} -> Resources;
        {_, cname_loop, Resources} -> Resources;
        {_, cname_referral, {_, _, Resources}} -> Resources;
        {_, nodata, {_, Resources}} -> Resources;
        {_, name_error, {_, Resources}} -> Resources;
        _ -> Resources=[]
    end,
    case lists:splitwith(fun ({_, cname, _, _, Domain}=CnameRr) -> not kurremkarmerruk_zone:authoritative_for(setelement(1, CnameRr, Domain), Namespace); (_) -> true end, lists:reverse(Resources)) of
        {_, []} ->
            override_authoritative(Rest, Namespace, [Tuple|Acc]);
        {Keep0, [CnameRr|_]} -> % This Rr has to follow from a cname.
            {_, Type, Class}=Question=element(1, Tuple),
            % When following cnames, only the domain can change. Thus we can take
            % Domain from the previous cname, Type and Class from the original question
            {_, _, _, _, CanonDomain} = CnameRr,
            Keep1 = [CnameRr|lists:reverse(Keep0)],
            AccTuple = case lists:last(kurremkarmerruk_zone:execute_query({CanonDomain, Type, Class}, Namespace)) of
                {_, ok, ZoneResources} -> {Question, ok, lists:append(ZoneResources, Keep1)};
                {_, cname, {ZoneCnameRr, ZoneResources}} -> {Question, cname, {ZoneCnameRr, lists:append(ZoneResources, Keep1)}};
                {_, cname_loop, ZoneResources} -> {Question, cname_loop, lists:append(ZoneResources, Keep1)};
                {Question, cname_referral, {ZoneCnameRr, Referral, ZoneResources}} ->
                    {Question, cname_referral, {ZoneCnameRr, Referral, lists:append(ZoneResources, Keep1)}};
                {_, AnswerType, {SoaRr, ZoneResources}} when AnswerType =:= nodata; AnswerType =:= name_error ->
                    {Question, AnswerType, {SoaRr, lists:append(ZoneResources, Keep1)}};
                {_, AnswerType, _}=Referral when AnswerType =:= referral; AnswerType =:= addressless_referral ->
                    {Question, cname_referral, {CnameRr, Referral, Keep1}}
            end,
            override_authoritative(Rest, Namespace, [AccTuple|Acc])
    end.


% We should probably include class in our consideration of authoritative entries...
authoritative_for({Question, _, _}, Namespace) when is_tuple(Question) ->
    authoritative_for(Question, Namespace);
authoritative_for(QuestionResource, #{namespace_id := NSID, zone_config := #{module := Storage}}) ->
    kurremkarmerruk_zone_storage:authoritative_for(?QUESTION_DOMAIN(QuestionResource), ?QUESTION_CLASS(QuestionResource), Storage, NSID);
authoritative_for(_, #{}) ->
    false.
