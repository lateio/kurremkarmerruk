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
-module(kurremkarmerruk_zone_server).

-behavior(gen_server).
-export([init/1,code_change/3,terminate/2,handle_call/3,handle_cast/2,handle_info/2]).

-export([
    start_link/0,
    storage_module/0
]).

% Functions used by zone transfer processes
-export([
    transfer_complete/1,
    transfer_error/3
]).

% Zone loading/modification/deletion
-export([
    new_zone_file/3,
    new_zone_dir/3,
    new_zone_transfer/5,
    new_zone_terms/2,
    delete_zone/2,
    delete_zone/3
]).

-include_lib("kernel/include/logger.hrl").
-include_lib("dnslib/include/dnslib.hrl").
-include_lib("dnslib/include/dnsfile.hrl").

-record(server_state, {
    storage,
    ongoing_transfers=#{},
    zones=[],
    dirs=[]
}).


-record(zone_terms, {
    terms = []
}).


-record(zone_file, {
    path        :: [binary()],
    included=[] :: [binary()],    % Other related files
    opts
}).


-record(zone_transfer, {
    fetched=false, % Whether we have ever successfully fetched the zone
    ref, % Replace with an id? Because there needs to be some way of linking events back to transfers
    primaries, % List of servers (And whether they've been tried recently?)
    opts,
    refresh = timer:minutes(5),
    refresh_timer,
    valid_until,
    valid_until_timer,
    cooldowns=[]
    % Keep a list of cooldowns for masters... Or a list of fresh / pending masters?
}).


-record(zone_transfer_event, {
    zone,
    pid,
    type = initial_fetch, % or refresh, retry...
    server,
    file,
    resolved=[]
}).


-record(zone_dir, {
    path,
    nsid = [],
    opts
}).


-record(zone, {
    apex         :: dnslib:domain(),
    class        :: dnsclass:class(),
    current_soa  :: dnslib:resource(),
    nsid,
    secondaries = [] :: {}, % Keep records of servers which have transferred the domain, in order to NOTIFY them of changes if required (Option to notify zone nameservers?)
    source       :: #zone_file{} | #zone_transfer{}
}).

-define(MIN_REFRESH, 60).
-define(MIN_EXPIRE, (?MIN_REFRESH * 3)).
-define(MIN_RETRY, 60).
-define(DEF_RETRY, 300).


%%
%%%% gen_server
%%


init([]) ->
    % Init storage here
    % Where will we keep zone files?
    process_flag(trap_exit, true),
    Module = ets_zone_storage,
    ModuleState = Module:init(rr, []),
    {ok, #server_state{storage={Module, ModuleState}}}.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


terminate(_, #server_state{}) ->
    % Somehow close storage?
    ok.


handle_call({zone_transfer, Apex, Class, Primaries0, Opts, NSID}, _, State = #server_state{zones=Zones, ongoing_transfers=Events, storage=Storage}) ->
    % For now, just support full zone transfer -> Makes it simple to just write the
    % records into a file...
    case is_new_zone(Apex, Class, NSID, Zones) of
        false -> {reply, {error, zone_already_exists}, State};
        true ->
            Primaries = lists:sort(Primaries0),
            #zone{nsid=NSIDs,source=#zone_transfer{ref=ZoneRef}} = Zone0
                = case [GenZone || GenZone = #zone{apex=GenApex,class=GenClass,source=#zone_transfer{primaries=GenPrimaries}} <- Zones, GenApex =:= Apex, GenClass =:= Class, GenPrimaries =:= Primaries] of
                    %[CaseZone = #zone{nsid=NSIDs}] -> CaseZone#zone{nsid=[NSID|NSIDs]}; % What if the zone has already been fetched...?
                    [] ->
                        #zone{
                            apex   = Apex,
                            class  = Class,
                            nsid   = [],
                            source = #zone_transfer{
                                fetched   = false,
                                ref       = make_ref(),
                                primaries = Primaries,
                                opts      = Opts
                                % Need some default retry for cases where the server does not respond...
                            }
                        }
                end,
            Zone1 = Zone0#zone{nsid=[NSID|NSIDs]},
            case
                case lists:member(nocache, Opts) of
                    true -> transfer;
                    false ->
                        case load_cached_zone_transfer(Apex, Class, Primaries) of
                            {ok, CacheFile, CacheExpire} -> {load, CacheFile, CacheExpire};
                            _ -> transfer
                        end
                end
            of
                {load, File, UntilExpire} when UntilExpire > 10 ->
                    % Log that we loaded a cached version of the zone
                    {ok, Rrs} = dnsfile:consult(File),
                    kurremkarmerruk_zone_storage:store_zone(Rrs, Storage, NSID),
                    Zone2 = case Zone1 of
                        #zone{source=Source=#zone_transfer{refresh_timer=undefined}} ->
                            Soa = hd([GenRR || GenRR <- Rrs, ?RESOURCE_TYPE(GenRR) =:= soa]),
                            SoaRefresh = dnsrr_soa:refresh(Soa),
                            Refresh = if
                                SoaRefresh < UntilExpire -> SoaRefresh;
                                true -> 0
                            end,
                            {ok, RefreshRef} = timer:send_after(timer:seconds(Refresh), {refresh, Zone1#zone.source#zone_transfer.ref}),
                            {ok, ExpireRef} = timer:send_after(timer:seconds(UntilExpire), {expire, Zone1#zone.source#zone_transfer.ref}),
                            Zone1#zone{
                                current_soa=Soa,
                                source=Source#zone_transfer{
                                    fetched=true,
                                    valid_until_timer=ExpireRef,
                                    refresh_timer=RefreshRef,
                                    refresh=erlang:monotonic_time(millisecond) + timer:seconds(dnsrr_soa:refresh(Soa)),
                                    valid_until=erlang:monotonic_time(millisecond) + timer:seconds(dnsrr_soa:expire(Soa))
                            }};
                        _ -> Zone1
                    end,
                    {reply, ok, State#server_state{zones=[Zone2|lists:delete(Zone0, Zones)]}};
                _ -> % Transfer
                    case Events of
                        #{ZoneRef := _} -> {reply, ok, State#server_state{zones=[Zone1|lists:delete(Zone0, Zones)]}}; % Something already going on
                        #{} -> {reply, ok, start_transfer(Zone1, State#server_state{zones=lists:delete(Zone0, Zones)})}
                    end
            end
    end;
handle_call({zone_file, Path, Opts, NSID}, _, State0) ->
    case add_file(Path, Opts, NSID, State0) of
        {ok, Soa, State1} -> {reply, {ok, Soa}, State1};
        {error, _}=Tuple -> {reply, Tuple, State0}
    end;
handle_call({zone_terms, Terms, NSID}, _, State = #server_state{storage=Storage, zones=Zones}) ->
    case dnszone:is_valid(Terms) of
        true ->
            Soa = hd([GenRR || GenRR <- Terms, ?RESOURCE_TYPE(GenRR) =:= soa]),
            Apex = dnslib:normalize_domain(?RESOURCE_DOMAIN(Soa)),
            Class = ?RESOURCE_CLASS(Soa),
            case is_new_zone(Apex, Class, NSID, Zones) of
                false -> {reply, {error, zone_already_exists}, State};
                true ->
                    Zone = #zone{
                        apex  = Apex,
                        class = Class,
                        nsid  = [NSID],
                        source = #zone_terms{
                            terms = lists:sort(Terms)
                        }
                    },
                    kurremkarmerruk_zone_storage:store_zone(Terms, Storage, NSID),
                    {reply, {ok, Soa}, State#server_state{zones=[Zone|Zones]}}
            end;
        false -> {reply, {error, invalid_zone}, State}
    end;
handle_call({zone_dir, Path, Opts, NSID}, _, State0) ->
    case add_dir([Path], Opts, NSID, [], State0) of
        {ok, Soas, State1} -> {reply, {ok, Soas}, State1};
        {error, _}=Tuple -> {reply, Tuple, State0}
    end;
handle_call({delete_zone, Apex, Class, NSID}, _, State = #server_state{storage=Storage}) ->
    ok = kurremkarmerruk_zone_storage:delete_zone(Apex, Class, Storage, NSID),
    {reply, ok, State};
handle_call(storage_module, _, State = #server_state{storage=Storage}) ->
    {reply, Storage, State};
handle_call(_, _, State) ->
    {noreply, State}.


handle_cast({transfer_complete, Ref}, State0 = #server_state{storage=Storage, zones=Zones, ongoing_transfers=Events}) ->
    #zone_transfer_event{server={Address, _, _}} = Event = maps:get(Ref, Events),
    State1 = case dnszone:is_valid_file(Event#zone_transfer_event.file, [return_soa]) of
        {true, Soa} ->
            {ok, Resources} = dnsfile:consult(Event#zone_transfer_event.file, [{allow_unknown_resources, true}, {allow_unknown_classes, true}]),
            Zone0 = #zone{apex=Apex,class=Class,source=Source=#zone_transfer{refresh_timer=RefreshRef, valid_until_timer=ExpireRef, opts=Opts, primaries=Servers}} =
                hd([GenZone || GenZone = #zone{source=#zone_transfer{ref=GenRef}} <- Zones, GenRef =:= Ref]),
            NSIDs = Zone0#zone.nsid,
            case Zone0#zone.source#zone_transfer.fetched of
                false -> ok = kurremkarmerruk_zone_storage:store_zone(Resources, Storage, NSIDs);
                true -> ok = kurremkarmerruk_zone_storage:update_zone(Resources, Storage, NSIDs)
            end,
            Refresh = max(dnsrr_soa:refresh(Soa), ?MIN_REFRESH),
            Expire = max(max(dnsrr_soa:expire(Soa), ?MIN_EXPIRE), Refresh), % Don't allow Expire to be smaller than Refresh... Though equal is not really much better
            case RefreshRef of
                undefined -> ok;
                _ -> timer:cancel(RefreshRef)
            end,
            case ExpireRef of
                undefined -> ok;
                _ -> timer:cancel(ExpireRef)
            end,
            % We should really fail/enforce some minimums if the transferred zone
            % has been improperly configured with refresh 0, expire 0, retry 0 or minimum 0...
            % In such a case there's rather no point in transferring a zone, is there?
            {ok, RefreshTimerRef} = timer:send_after(timer:seconds(Refresh), {refresh, Zone0#zone.source#zone_transfer.ref}),
            {ok, ExpireTimerRef} = timer:send_after(timer:seconds(Expire), {expire, Zone0#zone.source#zone_transfer.ref}),
            Zone1 = Zone0#zone{
                current_soa = Soa,
                source = Source#zone_transfer{
                    fetched     = true,
                    valid_until = erlang:monotonic_time(millisecond) + timer:seconds(Expire),
                    valid_until_timer = ExpireTimerRef,
                    refresh     = erlang:monotonic_time(millisecond) + timer:seconds(Refresh),
                    refresh_timer = RefreshTimerRef
                }
            },
            case lists:member(no_cache, Opts) of
                true -> ok;
                false -> cache_zone_transfer(Soa, Address, Servers, Event#zone_transfer_event.file)
            end,
            ok = file:delete(Event#zone_transfer_event.file),
            ServerStr = case is_list(Address) of
                true -> dnslib:domain_to_list(Address);
                false -> inet:ntoa(Address)
            end,
            ?LOG_INFO("Successfully transferred zone ~s ~p from server ~s. Refresh in ~B seconds, expire in ~B seconds", [dnslib:domain_to_list(Apex), Class, ServerStr, Refresh, Expire]),
            % Should log that we completed the transfer. Mebbe also log when we started the transfer.
            State0#server_state{zones=[Zone1|lists:delete(Zone0, Zones)], ongoing_transfers=maps:remove(Ref, Events)}
    end,
    {noreply, State1};
handle_cast({transfer_error, Ref, Type, Reason}, State0 = #server_state{ongoing_transfers=Events, zones=Zones}) ->
    case Events of
        #{Ref := Event} ->
            #zone_transfer_event{server={Address, _, _}} = Event,
            #zone{apex=Apex,class=Class,current_soa=Soa,source=Source=#zone_transfer{cooldowns=Cooldowns}} = Zone0 =
                hd([GenZone || GenZone = #zone{source=#zone_transfer{ref=GenRef}} <- Zones, GenRef =:= Ref]),
            CooldownLapses = erlang:monotonic_time(millisecond) +
                case Soa of
                    undefined -> timer:seconds(?DEF_RETRY);
                    _ -> timer:seconds(max(dnsrr_soa:retry(Soa), ?MIN_RETRY))
                end,
            AddressStr = case is_list(Address) of
                true -> dnslib:domain_to_list(Address);
                false -> inet:ntoa(Address)
            end,
            ?LOG_WARNING("Transfer error ~p ~p when trying to transfer zone ~s ~p from server ~s", [Type, Reason, dnslib:domain_to_list(Apex), Class, AddressStr]),
            Zone1 = Zone0#zone{source=Source#zone_transfer{cooldowns=[{Address, CooldownLapses}|Cooldowns]}},
            State1 = start_transfer(Zone1, State0#server_state{ongoing_transfers=maps:remove(Ref, Events), zones=lists:delete(Zone0, Zones)}),
            {noreply, State1}
    end;
handle_cast(_, State) ->
    {noreply, State}.


handle_info({file_event, Path, Event}, State0 = #server_state{storage=Storage, zones=Zones}) ->
    % Based on Filename, figure out which zone/nsids were affected. Then, based on event
    % either delete them, read Soa to figure out if we should reload them or if
    % the file was created, read it to figure out if if does contain a valid zone
    State1 = case [GenZone || GenZone = #zone{source=#zone_file{path=GenPath}} <- Zones, GenPath =:= Path] of
        [] -> State0; % Something's fishy... Check in included files
        [Zone = #zone{apex=Apex, class=Class, nsid=NSIDs, current_soa=CurrentSoa, source=#zone_file{opts=Opts}}] ->
            case Event of
                change ->
                    case dnszone:is_valid_file(Path, [return_soa]) of
                        {false, _} ->
                             % Log error?
                            State0;
                        {true, NewSoa} ->
                            CurrentSerial = dnsrr_soa:serial(CurrentSoa),
                            DisrespectSerial = lists:member(watch_ignore_serial, Opts),
                            case dnsrr_soa:serial(NewSoa) =/= CurrentSerial orelse DisrespectSerial of
                                true ->
                                    {_, DnsfileOpts} = lists:partition(fun add_file_opt/1, Opts),
                                    {ok, Resources} = dnsfile:consult(Path, DnsfileOpts),
                                    lists:map(fun (NSID) -> ok = kurremkarmerruk_zone_storage:update_zone(Resources, Storage, NSID) end, NSIDs),
                                    State0#server_state{zones=[Zone#zone{current_soa=NewSoa}|lists:delete(Zone, Zones)]};
                                false -> State0
                            end
                    end;
                delete ->
                    lists:map(fun (NSID) -> ok = kurremkarmerruk_zone_storage:delete_zone(Apex, Class, Storage, NSID) end, NSIDs),
                    State0#server_state{zones=lists:delete(Zone, Zones)}
            end
    end,
    {noreply, State1};
handle_info({dir_event, Filename, Event}, State) ->
    io:format("File event ~p on file ~s~n", [Event, Filename]),
    % Figure out which dir the file is under... -> run dirname until something matches
    case dnszone:is_valid_file(Filename) of
        {false, _} -> io:format("Not a valid zone~n"); % Do we have any hope that these will at some point evolve into proper DNS files?
        true -> io:format("A valid zone!~n")
    end,
    {noreply, State};
handle_info({refresh, Ref}, State0 = #server_state{zones=Zones, ongoing_transfers=Events}) ->
    % Instead of just rushing to refresh the whole zone, do an internal resolve for the
    % SOA record of the zone. Then compare serials and decide whether a complete transfer is actually warranted...
    Zone = hd([GenZone || GenZone = #zone{source=#zone_transfer{ref=GenRef}} <- Zones, GenRef =:= Ref]),
    #zone{
        apex=Apex,
        class=Class,
        current_soa=Soa
    } = Zone,
    case Soa of
        undefined ->
            State1 = start_transfer(Zone, State0#server_state{zones=lists:delete(Zone, Zones)}),
            {noreply, State1};
        {_, _, _, _, _} ->
            {ok, _, RefreshRef} = kurremkarmerruk:resolve(dnslib:question(Apex, soa, Class), [async, no_cache_lookup]),
            Event = #zone_transfer_event{
                zone=Ref,
                type = refresh
            },
            {noreply, State0#server_state{ongoing_transfers=Events#{RefreshRef => Event}}}
    end;
handle_info({expire, Ref}, State = #server_state{zones=Zones, storage=Storage}) ->
    Zone = hd([GenZone || GenZone = #zone{source=#zone_transfer{ref=GenRef}} <- Zones, GenRef =:= Ref]),
    #zone{
        apex=Apex,
        class=Class,
        nsid=NSIDs,
        source=Source
    } = Zone,
    case erlang:monotonic_time(millisecond) >= Zone#zone.source#zone_transfer.valid_until of
        true ->
            kurremkarmerruk_zone_storage:delete_zone(Apex, Class, Storage, NSIDs),
            % Need to mark the zone as missing/error/etc, so that kurremkarmerruk_zone can produce a SERVFAIL
            % Don't remove zone record, as we'll be trying to refresh it later on...
            {noreply, State#server_state{zones=[Zone#zone{source=Source#zone_transfer{fetched=false}}|lists:delete(Zone, Zones)]}};
        false -> {noreply, State}
    end;
handle_info({dns, DnsRef, Results}, State0 = #server_state{zones=Zones, ongoing_transfers=Events}) ->
    #zone_transfer_event{zone=ZoneRef} = maps:get(DnsRef, Events),
    #zone{current_soa=CurrentSoa, source=Source=#zone_transfer{valid_until_timer=PrevExpireRef}, apex=Apex, class=Class} = Zone0
        = hd([GenZone || GenZone = #zone{source=#zone_transfer{ref=GenRef}} <- Zones, GenRef =:= ZoneRef]),
    case hd(Results) of
        {_, ok, [NewSoa]} ->
            Newer = dnsrr_soa:serial_compare(CurrentSoa, NewSoa),
            if
                Newer ->
                    ?LOG_INFO("Based on SOA serials (ours: ~B, fetched: ~B), starting zone transfer for the new version of ~s ~p zone", [dnsrr_soa:serial(CurrentSoa), dnsrr_soa:serial(NewSoa), dnslib:domain_to_list(Apex), Class]),
                    {noreply, start_transfer(Zone0, State0#server_state{zones=lists:delete(Zone0, Zones), ongoing_transfers=maps:remove(DnsRef, Events)})};
                not Newer ->
                    timer:cancel(PrevExpireRef),
                    {ok, RefreshRef} = timer:send_after(timer:seconds(dnsrr_soa:refresh(CurrentSoa)), {refresh, Zone0#zone.source#zone_transfer.ref}),
                    {ok, ExpireRef} = timer:send_after(timer:seconds(dnsrr_soa:expire(CurrentSoa)), {expire, Zone0#zone.source#zone_transfer.ref}),
                    Zone1 = Zone0#zone{
                        source = Source#zone_transfer{
                            valid_until = erlang:monotonic_time(millisecond) + timer:seconds(dnsrr_soa:expire(CurrentSoa)),
                            valid_until_timer = ExpireRef,
                            refresh     = erlang:monotonic_time(millisecond) + timer:seconds(dnsrr_soa:refresh(CurrentSoa)),
                            refresh_timer = RefreshRef
                        }
                    },
                    % Log if new soa differs otherwise from the current one
                    ?LOG_INFO("Based on SOA serials (ours: ~B, fetched: ~B), keeping the current version of ~s ~p zone", [dnsrr_soa:serial(CurrentSoa), dnsrr_soa:serial(NewSoa), dnslib:domain_to_list(Apex), Class]),
                    {noreply, State0#server_state{zones=[Zone1|lists:delete(Zone0, Zones)], ongoing_transfers=maps:remove(DnsRef, Events)}}
            end;
        {_, _}=Tuple -> % An internal error
            Retry = dnsrr_soa:retry(CurrentSoa),
            {ok, RefreshRef} = timer:send_after(timer:seconds(Retry), {refresh, Zone0#zone.source#zone_transfer.ref}),
            Zone1 = Zone0#zone{
                source = Source#zone_transfer{
                    refresh = erlang:monotonic_time(millisecond) + timer:seconds(Retry),
                    refresh_timer = RefreshRef
                }
            },
            ?LOG_ERROR("An error occurred while trying to check if zone ~s ~p should be refreshed: ~p", [dnslib:domain_to_list(Apex), Class, Tuple]),
            {noreply, State0#server_state{zones=[Zone1|lists:delete(Zone0, Zones)], ongoing_transfers=maps:remove(DnsRef, Events)}}
    end;
handle_info({'EXIT', Pid, Exit}, State = #server_state{zones=Zones, ongoing_transfers=Events}) ->
    case Exit of
        normal -> {noreply, State};
        _ ->
            case [GenEvent || {_, GenEvent = #zone_transfer_event{pid=GenPid}} <- maps:to_list(Events), GenPid =:= Pid] of
                [] -> {noreply, State};
                CaseEvents ->
                    ?LOG_ERROR("Zone transfer worker exited abnormally with reason ~p", [Exit]),
                    #zone_transfer_event{zone=ZoneRef,file=File,server={Address, _, _}} = hd(CaseEvents),
                    ok = file:delete(File),
                    #zone{source=Source=#zone_transfer{cooldowns=Cooldowns},current_soa=Soa} = Zone =
                        hd([GenZone || GenZone = #zone{source=#zone_transfer{ref=GenRef}} <- Zones, GenRef =:= ZoneRef]),
                    CooldownLapses = erlang:monotonic_time(millisecond) +
                        case Soa of
                            undefined -> timer:seconds(?DEF_RETRY);
                            _ -> timer:seconds(max(dnsrr_soa:retry(Soa), ?MIN_RETRY))
                        end,
                    {noreply, start_transfer(Zone#zone{source=Source#zone_transfer{cooldowns=[{Address, CooldownLapses}|Cooldowns]}}, State#server_state{zones=lists:delete(Zone, Zones), ongoing_transfers=maps:remove(ZoneRef, Events)})}
            end
    end;
handle_info(Msg, State) ->
    io:format("kurremkarmerruk_zone_server message ~p~n", [Msg]),
    {noreply, State}.


%%
%%%% External API
%%


start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


storage_module() ->
    gen_server:call(?MODULE, storage_module).


new_zone_transfer(Apex, Class, Primaries, Opts, NSID) ->
    % It's possible for this (and all other new_* calls to timeout if the loaded zones are large enough on
    % a busy enough system...)
    gen_server:call(?MODULE, {zone_transfer, dnslib:normalize_domain(Apex), dnslib:class(Class), Primaries, Opts, NSID}).


% Allow the same opts as dnsfile:read_file, but with the addition of
% watch - Monitor the file (and files included by it for changes)
% watch_ignore_serial - Reload the file after writes even if the serial did not change
new_zone_file(Path, Opts, NSID) ->
    case filename:pathtype(Path) of
        relative -> {error, relative_path};
        absolute -> gen_server:call(?MODULE, {zone_file, Path, Opts, NSID})
    end.


% Allow the same opts as in load_file, but with the addition of
% watch_dir - Watch the directory for changes
% norecurse - Disallow recursing into the directory
new_zone_dir(Path, Opts, NSID) ->
    case filename:pathtype(Path) of
        relative -> {error, relative_path};
        absolute -> gen_server:call(?MODULE, {zone_dir, Path, Opts, NSID})
    end.


new_zone_terms(Terms, NSID) ->
    gen_server:call(?MODULE, {zone_terms, Terms, NSID}).


delete_zone(Soa, NSID) when ?IS_RESOURCE(Soa) ->
    delete_zone(?RESOURCE_DOMAIN(Soa), ?RESOURCE_CLASS(Soa), NSID).

delete_zone(Apex, Class, NSID) ->
    gen_server:call(?MODULE, {delete_zone, Apex, Class, NSID}).


transfer_complete(Ref) ->
    gen_server:cast(?MODULE, {transfer_complete, Ref}).


transfer_error(Ref, ErrorType, Reason) ->
    gen_server:cast(?MODULE, {transfer_error, Ref, ErrorType, Reason}).


%%
%%%% Internal functions
%%


add_file(Path, Opts0, NSID, State0 = #server_state{zones=Zones, storage=Storage}) ->
    % Prune out opts from read_file opts... watch, what others?
    % Prune Our opts...
    {Opts, DnsfileOpts} = lists:partition(fun add_file_opt/1, Opts0),
    case dnszone:is_valid_file(Path, [return_soa|DnsfileOpts]) of
        {false, Reason} -> {error, Reason};
        {true, Soa} ->
            RootFile = unicode:characters_to_binary(Path, unicode, file:native_name_encoding()),
            Apex = dnslib:normalize_domain(?RESOURCE_DOMAIN(Soa)),
            Class = ?RESOURCE_CLASS(Soa),
            case [GenZone || GenZone <- Zones, GenZone#zone.apex =:= Apex, GenZone#zone.class =:= Class] of
                [] ->
                    % Gonna be a new zone
                    {ok, Files} = dnsfile:read_file(Path, DnsfileOpts),
                    {Resources, RootFile, IncludedFiles} = lists:foldl(fun add_file_fold_files/2, {[], RootFile, []}, Files),
                    Soa = hd([GenRR || GenRR <- Resources, ?RESOURCE_TYPE(GenRR) =:= soa]),
                    Zone = #zone{
                        apex        = Apex,
                        class       = Class,
                        current_soa = Soa,
                        nsid        = [NSID],
                        source      = #zone_file{
                            path     = RootFile,
                            included = IncludedFiles,
                            opts     = Opts0 % If there are multiple NSIDs, the opts of which will we respect? Currently only the first caller.
                        }
                    },
                    case lists:member(watch, Opts) of
                        true -> ok;
                            % ok = kurremkarmerruk_zone_fs_watcher:watch_file(RootFile);
                            % Would also need to watch any included files...
                        false -> ok
                    end,
                    kurremkarmerruk_zone_storage:store_zone(Resources, Storage, NSID),
                    ?LOG_INFO("Loaded zone ~s ~p from file ~s", [dnslib:domain_to_list(Apex), Class, Path]),
                    {ok, Soa, State0#server_state{zones=[Zone|Zones]}};
                Matching ->
                    % Before trying to locate zones sharing the root path, we need to check for zones
                    % where NSID is already present...
                    %case [GenZone || GenZone <- Matching, GenZone#zone.source#zone_file.path =:= RootFile] of
                    case [GenZone || GenZone <- Matching, lists:member(NSID, GenZone#zone.nsid)] of
                        [] ->
                            % Not yet a zone with this file as the source
                            % also, recursing makes us verify the file all over again...
                            case [GenZone || GenZone <- Matching, GenZone#zone.source#zone_file.path =:= RootFile] of
                                [] ->
                                    {ok, State1 = #server_state{zones=NewZones}} = add_file(Path, Opts0, NSID, State0#server_state{zones=[]}),
                                    ?LOG_INFO("Loaded zone ~s ~p from file ~s", [dnslib:domain_to_list(Apex), Class, Path]),
                                    {ok, Soa, State1#server_state{zones=lists:append(NewZones, Zones)}};
                                [Zone = #zone{nsid=NSIDs}] ->
                                    case Zone#zone.current_soa of
                                        Soa -> % If the soa matches our current one, no need to refresh other stored copies of the zone...
                                            ?LOG_INFO("Loaded zone ~s ~p from file ~s", [dnslib:domain_to_list(Apex), Class, Path]),
                                            {ok, _} = add_file(Path, Opts0, NSID, State0#server_state{zones=[]}),
                                            {ok, Soa, State0#server_state{zones=[Zone#zone{nsid=[NSID|NSIDs]}|lists:delete(Zone, Zones)]}}
                                    end
                            end;
                        [NSIDZone = #zone{source=#zone_file{}}] ->
                            % What if the file belongs/includes other files?
                            % Since is_valid_file does not tell us anything about the other files
                            % and because we start search with apex/class, any superset (discovered) after
                            % a superset is just going to register as an error (another file trying to create
                            % the same )
                            case NSIDZone#zone.source#zone_file.path of
                                RootFile -> % #zone_file.path=RootFile would not match
                                    % Should we check Serial, in case we should reload?
                                    ?LOG_INFO("Loaded zone ~s ~p from file ~s", [dnslib:domain_to_list(Apex), Class, Path]),
                                    {ok, Soa, State0};
                                _ ->
                                    case lists:member(RootFile, NSIDZone#zone.source#zone_file.included) of
                                        true -> {error, {file_already_included_from_other, NSIDZone#zone.source#zone_file.path}}
                                        %false ->
                                            %
                                            % File, but path did not match... It's possible that our new file is a subset of the previous
                                            % Or it could be that our new file is a superset of the previous.
                                            % Look for RootPath in included files. If it is present, we can just skip the file
                                            % If it's not, we need to read_file the new file and check if the previous root was among the
                                            % the files it included...
                                            % Should we check Serial, in case we should reload?
                                    end
                            end;
                        _ -> {error, duplicate_zone} % Tried to load a zone, even though the apex and class are already attached to other source for the NSID
                    end
            end
    end.

add_file_opt(watch) -> true;
add_file_opt(watch_ignore_serial) -> true;
add_file_opt(_) -> false.

add_file_fold_files(#dnsfile{resources=FileResources, included_from=undefined, path=Path}, {Resources, _, IncludedFiles}) ->
    {lists:append(FileResources, Resources), unicode:characters_to_binary(Path, unicode, file:native_name_encoding()), IncludedFiles};
add_file_fold_files(#dnsfile{resources=FileResources, path=Path}, {Resources, RootFile, IncludedFiles}) ->
    {lists:append(FileResources, Resources), RootFile, [unicode:characters_to_binary(Path, unicode, file:native_name_encoding())|IncludedFiles]}.


add_dir([Path] = Paths, Opts0, NSID, Acc, State) ->
    {Opts, DnsfileOpts} = lists:partition(fun add_dir_opt/1, Opts0),
    Dir = #zone_dir{
        path = unicode:characters_to_binary(Path, unicode, file:native_name_encoding()),
        nsid = [NSID]
    },
    % Are we gonna watch the dir?
    add_dir(Paths, [Dir|Opts], DnsfileOpts, NSID, Acc, State, 0).

add_dir(_, _, _, _, _, _, StepLimit) when StepLimit > 500 ->
    {error, {traversed_obscene_levels_of_subdirs, StepLimit}};
add_dir([], Opts, _, _, Acc, State = #server_state{dirs=Dirs}, _) ->
    [Dir] = [GenOpt || GenOpt = #zone_dir{} <- Opts],
    {ok, Acc, State#server_state{dirs=[Dir|Dirs]}};
add_dir([Path|Rest0], Opts, DnsfileOpts, NSID, Acc0, State0, StepLimit) ->
    FilterFn = case lists:member(dotfiles, Opts) of
        true -> fun (_) -> true end;
        false -> fun (FunStr) -> hd(FunStr) =/= $. end
    end,
    {ok, Files0} = file:list_dir(Path),
    Files = lists:sort([GenFile || GenFile <- Files0, FilterFn(GenFile)]), % Sort and, depending on settings, remove dotfiles
    {RootFiles, _Included, Dirs} = lists:foldl(add_dir_fold_fn(Path), {[], [], []}, Files),
    case add_dir_file(RootFiles, Opts, DnsfileOpts, NSID, Acc0, State0) of
        {ok, Acc1, State1} ->
            % If dir is to be watched, we should really just 'truly' watch top
            % parent dir... Should watch_drv take care of that? Or fs watcher?
            % Also, we'll need to add dir entry for first directory... Some opt which
            % we'll prune away?
            % Are we going to recurse? Should we, by default?
            Rest = case lists:member(recurse, Opts) of
                true -> lists:append(Dirs, Rest0);
                false -> Rest0
            end,
            add_dir(Rest, Opts, DnsfileOpts, NSID, Acc1, State1, StepLimit + 1);
        {error, _}=ErrTuple -> ErrTuple
    end.

add_dir_file([], _, _, _, Acc, State) ->
    {ok, Acc, State};
add_dir_file([RootFile|Rest], Opts, DnsfileOpts, NSID, Acc, State0) ->
    case add_file(RootFile, DnsfileOpts, NSID, State0) of
        {ok, Soa, State1} -> add_dir_file(Rest, Opts, DnsfileOpts, NSID, [Soa|Acc], State1);
        {error, Reason} ->
            ?LOG_WARNING("~p: Skipping invalid zone file ~s: ~p", [?MODULE, RootFile, Reason]),
            add_dir_file(Rest, Opts, DnsfileOpts, NSID, Acc, State0)
    end.

add_dir_opt(watch_dir) -> true;
add_dir_opt(dotfiles)  -> true;
add_dir_opt(recurse)   -> true;
add_dir_opt(_) -> false.

add_dir_fold_fn(DirPath) ->
    fun (File, {RootFiles, Includes, Dirs}) ->
        Path = filename:join(DirPath, File),
        case filelib:is_dir(Path) of
            true -> {RootFiles, Includes, [Path|Dirs]};
            false ->
                case dnsfile:read_file_includes(Path) of
                    {error, _} -> {RootFiles, Includes, Dirs};
                    {ok, Files} ->
                        {[RootFile], IncludedFiles0} = lists:partition(fun (FunFile) -> FunFile#dnsfile.included_from =:= undefined end, Files),
                        case lists:member(RootFile#dnsfile.path, Includes) of
                            true -> {RootFiles, Includes, Dirs};
                            false ->
                                IncludedFiles = [GenFile#dnsfile.path || GenFile <- IncludedFiles0],
                                {
                                    [RootFile#dnsfile.path|RootFiles],
                                    lists:append(IncludedFiles -- Includes, Includes),
                                    Dirs
                                }
                        end
                end
        end
    end.


is_new_zone(_, _, _, []) ->
    true;
is_new_zone(Apex, Class, NSID, [Zone|Rest]) ->
    case Zone of
        #zone{class=Class,apex=Apex,nsid=NSIDs} ->
            case lists:member(NSID, NSIDs) of
                false -> is_new_zone(Apex, Class, NSID, Rest);
                true -> false
            end;
        _ -> is_new_zone(Apex, Class, NSID, Rest)
    end.


start_transfer(Zone = #zone{source=Source}, State = #server_state{zones=Zones, ongoing_transfers=Events}) ->
    #zone_transfer{primaries=Primaries, cooldowns=Cooldowns0} = Source,
    Now = erlang:monotonic_time(millisecond),
    Cooldowns = [GenCool || GenCool <- Cooldowns0, Now < element(2, GenCool)],
    Fn = prune_available_fn(Cooldowns),
    case lists:filter(Fn, Primaries) of
        [] ->
            % No server available. Wait for the next earliest available
            Earliest = hd(lists:sort(fun sort_cooldown/2, Cooldowns)),
            {ok, TimerRef} = timer:send_after(element(2, Earliest) - Now, {refresh, Zone#zone.source#zone_transfer.ref}),
            State#server_state{zones=[Zone#zone{source=Source#zone_transfer{cooldowns=Cooldowns, refresh_timer=TimerRef}}|Zones]};
        AvailablePrimaries ->
            % Prefer tls/ip addresses over tcp and domain names
             {Address, Port, ServerOpts} = Server = transfer_choose_server(AvailablePrimaries),
             ZoneRef = Zone#zone.source#zone_transfer.ref,
             Event = #zone_transfer_event{
                     zone   = ZoneRef,
                     server = Server,
                     type   =
                         if
                             Zone#zone.source#zone_transfer.fetched -> refresh;
                             not Zone#zone.source#zone_transfer.fetched -> initial_fetch
                         end,
                     file   = string:trim(os:cmd("mktemp"))
             },
            ChildOpts = #{
                address => Address,
                port    => Port,
                cbinfo  =>
                    case lists:member(tls, ServerOpts) of
                        true -> {ssl,tcp,ssl_closed,ssl_error};
                        false -> {gen_tcp,tcp,tcp_closed,tcp_error}
                    end
            },
            Msg = dnsmsg:new(#{}, dnslib:question(Zone#zone.apex, axfr, Zone#zone.class)),
            {ok, Pid} = supervisor:start_child(kurremkarmerruk_zone_transfer_sup, [self(), ChildOpts, Msg, ZoneRef, Event#zone_transfer_event.file]),
            State#server_state{zones=[Zone|Zones], ongoing_transfers=Events#{ZoneRef => Event#zone_transfer_event{pid=Pid}}}
    end.


prune_available_fn(Cooldowns) ->
    CooldownAddresses = [element(1, GenCool) || GenCool <- Cooldowns],
    fun (Server) -> not lists:member(element(1, Server), CooldownAddresses) end.

sort_cooldown({_, Expiry1}, {_, Expiry2}) -> Expiry1 < Expiry2.

transfer_choose_server(Servers) ->
    transfer_choose_server([tls, ip_address], Servers).

transfer_choose_server(_, []) ->
    false;
transfer_choose_server([], Servers) ->
    hd(Servers);
transfer_choose_server([Param|Rest], Servers) ->
    case transfer_choose_server(Param, Servers) of
        false -> transfer_choose_server(Rest, Servers);
        Server -> Server
    end;
transfer_choose_server(tls, [Server|Rest]) ->
    case lists:member(tls, element(3, Server)) of
        true -> Server;
        false -> transfer_choose_server(tls, Rest)
    end;
transfer_choose_server(ip_address, [Server|Rest]) ->
    case dnslib:is_valid_domain(element(1, Server)) of
        true -> transfer_choose_server(ip_address, Rest);
        _ -> Server
    end.


cache_zone_transfer(Soa, Address, Servers, TmpFile) ->
    case application:get_env(kurremkarmerruk, data_dir) of
        {ok, DataDir} ->
            Fn = fun (FunPart, FunPath0) ->
                FunPath1 = filename:join(FunPath0, FunPart),
                case file:make_dir(FunPath1) of
                    ok -> FunPath1;
                    {error, eexist} -> FunPath1;
                    {error, Reason} -> throw(Reason)
                end
            end,
            try lists:foldl(Fn, DataDir, ["cache", "zone_transfer"]) of
                CacheDir -> cache_zone_transfer(Soa, Address, Servers, TmpFile, CacheDir, [])
            catch
                Reason ->
                    ?LOG_ERROR("Failed to create directories in Kurremkarmerruk data_dir: ~p", [Reason]),
                    error
            end;
        _ ->
            ?LOG_ERROR("Not caching zone transfer because data_dir is not configured"),
            error
    end.

cache_zone_transfer(Soa, Address0, [], TmpFile, CacheDir, DeletedFiles) ->
    Apex = dnslib:domain_to_list(dnslib:normalize_domain(?RESOURCE_DOMAIN(Soa))),
    Class = ?RESOURCE_CLASS(Soa),
    Address1 = case is_list(Address0) of
        true -> dnslib:domain_to_list((dnslib:normalize_domain(Address0)));
        false -> inet:ntoa(Address0)
    end,
    Filename = io_lib:format("zone-~s-~p-from-~s", [Apex, Class, Address1]),
    Path = filename:join(CacheDir, Filename),
    case file:copy(TmpFile, Path) of
        {ok, _} ->
            Index0 = hd(case file:consult(filename:join(CacheDir, "index")) of
                {ok, CaseIndex} -> CaseIndex;
                _ -> [#{}]
            end),
            Index1 = lists:foldl(fun (Key, FunMap) -> maps:remove(Key, FunMap) end, Index0, DeletedFiles),
            GoodUntil = erlang:system_time(second) + dnsrr_soa:expire(Soa),
            Index2 = Index1#{Filename => GoodUntil},
            % Write index
            ok = kurremkarmerruk_utils:write_terms(filename:join(CacheDir, "index"), [Index2])
    end;
cache_zone_transfer(Soa, Address, [{Address0, _, _}|Rest], TmpFile, CacheDir, DeletedFiles) ->
    % Figure out if there's already a file from one of the servers...
    Apex = dnslib:domain_to_list(dnslib:normalize_domain(?RESOURCE_DOMAIN(Soa))),
    Class = ?RESOURCE_CLASS(Soa),
    Address1 = case is_list(Address0) of
        true -> dnslib:domain_to_list((dnslib:normalize_domain(Address0)));
        false -> inet:ntoa(Address0)
    end,
    Filename = io_lib:format("zone-~s-~p-from-~s", [Apex, Class, Address1]),
    PossiblePath = filename:join(CacheDir, Filename),
    case file:delete(PossiblePath) of
        ok -> cache_zone_transfer(Soa, Address, Rest, TmpFile, CacheDir, [PossiblePath|DeletedFiles]);
        {error, enoent} -> cache_zone_transfer(Soa, Address, Rest, TmpFile, CacheDir, DeletedFiles);
        {error, Reason} ->
            ?LOG_ERROR("Tried to delete previously cached file ~s: ~p", [PossiblePath, Reason]),
            error
    end.


load_cached_zone_transfer(Apex, Class, Primaries) ->
    case application:get_env(kurremkarmerruk, data_dir) of
        {ok, DataDir} -> load_cached_zone_transfer(Apex, Class, Primaries, filename:join([DataDir, "cache", "zone_transfer"]));
        _ ->
            ?LOG_ERROR("Not loading zone transfer from cache because data_dir is not configured"),
            error
    end.

load_cached_zone_transfer(_, _Class, [], CacheDir) ->
    {ok, [Index0]} = file:consult(filename:join(CacheDir, "index")),
    Now = erlang:system_time(second),
    Index1 = clear_transfer_cache(Index0, Now, CacheDir),
    kurremkarmerruk_utils:write_terms(filename:join(CacheDir, "index"), [Index1]),
    error;
load_cached_zone_transfer(Apex0, Class, [{Address, _, _}|Rest], CacheDir) ->
    Apex1 = dnslib:domain_to_list(dnslib:normalize_domain(Apex0)),
    AddressStr = case is_list(Address) of
        true -> dnslib:domain_to_list(dnslib:normalize_domain(Address));
        false -> inet:ntoa(Address)
    end,
    Filename = io_lib:format("zone-~s-~p-from-~s", [Apex1, Class, AddressStr]),
    Path = filename:join(CacheDir, Filename),
    case filelib:is_regular(Path) of
        false -> load_cached_zone_transfer(Apex0, Class, Rest, CacheDir);
        true ->
            {ok, [Index0]} = file:consult(filename:join(CacheDir, "index")),
            Now = erlang:system_time(second),
            Index1 = clear_transfer_cache(Index0, Now, CacheDir),
            kurremkarmerruk_utils:write_terms(filename:join(CacheDir, "index"), [Index1]),
            case Index1 of
                #{Filename := GoodUntil} when GoodUntil > Now -> {ok, Path, GoodUntil - Now};
                #{} -> error
            end
    end.


clear_transfer_cache(Index, Now, CacheDir) ->
    {Keep, Delete} = lists:partition(fun ({_, GoodUntil}) -> GoodUntil >= Now end, maps:to_list(Index)),
    case Delete of
        [] -> Index;
        _ ->
            lists:map(fun (FunFile) -> file:delete(filename:join(CacheDir, FunFile)) end, [element(1, GenTuple) || GenTuple <- Delete]),
            maps:from_list(Keep)
    end.
