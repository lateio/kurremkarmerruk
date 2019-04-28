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
-module(kurremkarmerruk_recurse_address_reputation).

-behavior(gen_server).
-export([init/1,code_change/3,terminate/2,handle_call/3,handle_cast/2,handle_info/2]).


-export([
    start_link/0,
    address_rep_checker/3,
    update_address_reputation/3,
    address_connected/1,
    address_disconnected/1,
    report_error/2,
    address_reputation/1,
    address_reputations/0,
    referral_choose_address/2,
    referral_choose_address/3,
    referral_choose_address/4,
    referral_add_reputation_info/1,

    available_afs/0,
    address_family_unavailable/1,

    address_timeout/2,
    udp_timeout/2,
    inc_messages_sent/1,

    address_wait_timeout/1
]).

-include_lib("kurremkarmerruk/include/recurse.hrl").

-define(SIN_LIMIT, 20).


-record(server_state, {
    tab,
    address_rep_timer,
    address_rep_ref,
    address_family_timer,
    address_families=init_address_families(),
    af_sins=#{inet6 => 0, inet => 0}
}).
-record(address_reputation, {
    address,
    latest_latency,
    avg_latency,
    avg_latency_datum_count=0,
    max_latency,
    tried_udp_size=0,
    advertised_udp_size=512,
    created=erlang:monotonic_time(millisecond),
    last_successful_contact,
    last_error_contact,
    requests=0,
    successful=0,
    timeout=0,
    errors=0,
    connected=false,
    blocked_ports=[]
}).


init([]) ->
    process_flag(trap_exit, true),
    Tab = ets:new(dns_resolver_reputation, [named_table, {keypos, 2}, ordered_set]),
    {ok, AfTimer} = timer:send_interval(timer:seconds(30), refresh_available_address_families),
    {ok, RepTimer} = timer:send_interval(timer:minutes(1), refresh_reputations),
    {ok, #server_state{tab=Tab,address_rep_timer=RepTimer,address_family_timer=AfTimer}}.


code_change(_OldVsn, _State, _Extra) ->
    {ok, _State}.


terminate(_Reason, _State) ->
    ok.


handle_call(available_afs, _, State = #server_state{address_families=Afs,af_sins=Sins}) ->
    {reply, {Afs, Sins}, State};
handle_call(_, _, State) ->
    {noreply, State}.


handle_cast({address_family_unavailable, Af}, State = #server_state{af_sins=Sins0}) ->
    #{Af := Count0} = Sins0,
    Count1 = case Count0 of
        0 -> 10;
        _ -> Count0 * 2
    end,
    Sins1 = Sins0#{Af := Count1},
    {noreply, State#server_state{af_sins=Sins1}};
handle_cast({update_address_reputation, Address, Latency, UDPSize}, State = #server_state{tab=Tab}) ->
    % Is it possible that one address family is over and above the better one?
    EtsTuple = case ets:lookup(Tab, Address) of
        [] -> create_address_rep_entry(Tab, Address);
        [Tmp] -> Tmp
    end,
    Fields = address_reputation_fields(),
    true = ets:update_element(Tab, Address, [
        {maps:get(latest_latency, Fields), Latency},
        {maps:get(advertised_udp_size, Fields), UDPSize},
        {maps:get(last_successful_contact, Fields), erlang:monotonic_time(millisecond)},
        {maps:get(successful, Fields), element(maps:get(successful, Fields),EtsTuple)+1}
    ]),
    {noreply, State};
handle_cast({error, Address, Reason}, State = #server_state{tab=Tab}) ->
    EtsTuple = case ets:lookup(Tab, Address) of
        [] -> create_address_rep_entry(Tab, Address);
        [Tmp] -> Tmp
    end,
    Fields = address_reputation_fields(),
    Updates = case Reason of
        format_error -> [{maps:get(errors, Fields), element(maps:get(errors, Fields),EtsTuple)+10}];
        {econnrefused, _Proto, _Port} -> [{maps:get(errors, Fields), element(maps:get(errors, Fields),EtsTuple)+30}];
        missing_responses -> [{maps:get(errors, Fields), element(maps:get(errors, Fields),EtsTuple)+1}];
        unsolicited_message -> [{maps:get(errors, Fields), element(maps:get(errors, Fields),EtsTuple)+1}]
    end,
    true = ets:update_element(Tab, Address, [
        {maps:get(last_error_contact, Fields), erlang:monotonic_time(millisecond)}
        |Updates
    ]),
    {noreply, State};
handle_cast({address_timeout, Address, TriedUDPSize}, State) ->
    address_timeout(Address, TriedUDPSize, State),
    {noreply, State};
handle_cast({address_connected, Address}, State = #server_state{tab=Tab}) ->
    _EtsTuple = case ets:lookup(Tab, Address) of
        [] -> create_address_rep_entry(Tab, Address);
        [Tmp] -> Tmp
    end,
    Fields = address_reputation_fields(),
    true = ets:update_element(Tab, Address, [
        {maps:get(connected, Fields), true}
    ]),
    {noreply, State};
handle_cast({address_disconnected, Address}, State = #server_state{tab=Tab}) ->
    _EtsTuple = case ets:lookup(Tab, Address) of
        [] -> create_address_rep_entry(Tab, Address);
        [Tmp] -> Tmp
    end,
    Fields = address_reputation_fields(),
    true = ets:update_element(Tab, Address, [
        {maps:get(connected, Fields), false}
    ]),
    {noreply, State};
handle_cast({inc_messages_sent, Address}, State = #server_state{tab=Tab}) ->
    EtsTuple = case ets:lookup(Tab, Address) of
        [] -> create_address_rep_entry(Tab, Address);
        [Tmp] -> Tmp
    end,
    Fields = address_reputation_fields(),
    true = ets:update_element(Tab, Address, [
        {maps:get(requests, Fields), element(maps:get(requests, Fields),EtsTuple)+1}
    ]),
    {noreply, State};
handle_cast(_Msg, _State) ->
    {noreply, _State}.


handle_info(refresh_reputations, State = #server_state{tab=Tab,address_rep_ref=undefined,af_sins=Sins0}) ->
    Ref = make_ref(),
    spawn_link(?MODULE, address_rep_checker, [self(), Tab, Ref]),
    Sins1 = maps:map(fun (_, Value) when Value > 0 -> Value - 1; (_, Value) -> Value end, Sins0),
    {noreply, State#server_state{address_rep_ref=Ref,af_sins=Sins1}};
handle_info({address_rep_results, Ref, Drop, Decrease}, State = #server_state{tab=Tab,address_rep_ref=Ref}) ->
    Fields = address_reputation_fields(),
    [ets:delete(Tab, GenId) || GenId <- Drop],
    UpdateCounter = [{maps:get(errors, Fields), -1, 0, 0}, {maps:get(timeout, Fields), -1, 0, 0}],
    [ets:update_counter(Tab, GenId, UpdateCounter) || GenId <- Decrease],
    {noreply, State#server_state{address_rep_ref=undefined}};
handle_info(refresh_available_address_families, State) ->
    {noreply, State#server_state{address_families=init_address_families()}};
handle_info(_Msg, _State) ->
    {noreply, _State}.


start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


address_rep_checker(Pid, Tab, Ref) ->
    % Should somehow get rid of this table walk...
    TimeSinceContact = application:get_env(kurremkarmerruk, address_reputation_max_age, 1800),
    address_rep_checker([], [], ets:first(Tab), erlang:monotonic_time(millisecond), timer:seconds(TimeSinceContact), Pid, Tab, Ref).

address_rep_checker(Drop, Keep, '$end_of_table', _, _, Pid, _, Ref) ->
    Pid ! {address_rep_results, Ref, Drop, Keep},
    exit(normal);
address_rep_checker(Drop, Keep, Key, Now, MaxAge, Pid, Tab, Ref) ->
    case ets:lookup(Tab, Key) of
        [] -> address_rep_checker(Drop, Keep, ets:next(Tab, Key), Now, MaxAge, Pid, Tab, Ref);
        [#address_reputation{last_successful_contact=undefined,created=Created}] when Now - Created >= MaxAge ->
            address_rep_checker([Key|Drop], Keep, ets:next(Tab, Key), Now, MaxAge, Pid, Tab, Ref);
        [#address_reputation{last_successful_contact=Last}] when Now - Last >= MaxAge ->
            address_rep_checker([Key|Drop], Keep, ets:next(Tab, Key), Now, MaxAge, Pid, Tab, Ref);
        [#address_reputation{timeout=0,errors=0}] -> address_rep_checker(Drop, Keep, ets:next(Tab, Key), Now, MaxAge, Pid, Tab, Ref);
        _ -> address_rep_checker(Drop, [Key|Keep], ets:next(Tab, Key), Now, MaxAge, Pid, Tab, Ref)
    end.


address_family_unavailable(Af) ->
    gen_server:cast(?MODULE, {address_family_unavailable, Af}).


update_address_reputation(Address, Latency, UDPSize) ->
    gen_server:cast(?MODULE, {update_address_reputation, Address, Latency, UDPSize}).


address_connected(Address) ->
    gen_server:cast(?MODULE, {address_connected, Address}).

address_disconnected(Address) ->
    gen_server:cast(?MODULE, {address_disconnected, Address}).


report_error(Address, Reason) ->
    gen_server:cast(?MODULE, {error, Address, Reason}).


available_afs() ->
    {Afs, Sins} = gen_server:call(?MODULE, available_afs),
    available_afs(Afs, Sins).

available_afs(Afs, Sins) ->
    lists:sort(fun (Af1, Af2) -> maps:get(Af1, Sins) =< maps:get(Af2, Sins) end, Afs).


address_reputation(Address) ->
    case ets:lookup(dns_resolver_reputation, Address) of
        [] -> unknown;
        [Tuple] -> Tuple
    end.


address_reputations() ->
    ets:tab2list(dns_resolver_reputation).


udp_timeout(Address, TriedUDPSize) ->
    address_timeout(Address, TriedUDPSize).

address_timeout(Address, TriedUDPSize) ->
    gen_server:cast(?MODULE, {address_timeout, Address, TriedUDPSize}).

address_timeout(Address, _TriedUDPSize, #server_state{tab=Tab}) ->
    Fields = address_reputation_fields(),
    ets:update_counter(Tab, Address, {maps:get(timeout, Fields), 1}, #address_reputation{address=Address}).


inc_messages_sent(Address) ->
    gen_server:cast(?MODULE, {inc_messages_sent, Address}).


create_address_rep_entry(Tab, Address) ->
    Default = #address_reputation{address=Address},
    true = ets:insert_new(Tab, Default),
    Default.


referral_choose_address(Referral, Af) ->
    referral_choose_address(Referral, Af, #tried{}, none).

referral_choose_address(Referral, Af, Tried) ->
    referral_choose_address(Referral, Af, Tried, none).

referral_choose_address({{_, referral, NsAddressRrs}, referral_with_reputation_info, _}, [], #tried{}, _) ->
    case [GenNsRr || {GenNsRr, []} <- NsAddressRrs] of
        [] -> {error, no_suitable_address};
        NsRrs -> {addressless_nsrrs, NsRrs}
    end;
referral_choose_address({_, referral_with_reputation_info, AddressReps0}=Referral, [Af|OtherAf], Tried = #tried{servers=TriedServers,afs=TriedAfs}, ErrorTolerance) ->
    % Should choose from any of the offered addresses, not just the first ns...
    % We can't currently distinguish between tls and basic, although tried does contain that
    % information. We should be able to fail from tls to basic (if such option is provided)
    % Allow ServerSpec in referral addresses? Though we currently keep reputation
    % keyed only by address...
    case lists:member(Af, TriedAfs) of
        true -> referral_choose_address(Referral, OtherAf, Tried, ErrorTolerance);
        false ->
            TupleSize = case Af of
                inet -> 4;
                inet6 -> 8
            end,
            TriedAddresses = [GenAddress || {{_, {GenAddress, _}}, _} <- TriedServers],
            MaxErrors = case ErrorTolerance of
                none -> 0
            end,
            AddressReps = [GenTuple || {GenAddress, _}=GenTuple <- AddressReps0, tuple_size(GenAddress) =:= TupleSize, not lists:member(GenAddress, TriedAddresses)],
            Total = length(AddressReps),
            case
                % Other option is to consider whether we already know a reasonably low latency option (What is low enough should prolly be configurable)
                % If we have, don't bother contacting other servers
                % Or if we only know servers with unacceptably long latencies, try to contact new ones.
                % Or try to find fast addresses for active servers
                %
                % Also, it doesn't make sense restrict our choice of an address to just single address family...
                % Consider a case where one family consistently timeouts and other consistently responds. Even
                % though we might prefer the other family generally, there's no reason to choose the worse address
                % when we _know_ that to be the case...
                case lists:partition(fun ({_, FunRep}) -> FunRep =/= unknown end, AddressReps) of
                    {[], []} -> [];
                    {_, Unknown} when Total < 10, Unknown =/= [] -> Unknown;
                    {Known, Unknown} when length(Known) =< length(Unknown) -> Unknown;
                    {Known, Unknown} when length(Known) > length(Unknown) -> Known
                end
            of
                [] -> referral_choose_address(Referral, OtherAf, Tried, ErrorTolerance);
                [{_, Rep}|_] = ChooseFrom when Rep =:= unknown ->
                    % What if one of the addresses is local and others are not?
                    {Address, _} = lists:nth(rand:uniform(length(ChooseFrom)), ChooseFrom),
                    {ok, Address, address_wait_timeout(#address_reputation{address=Address})};
                ChooseFrom0 ->
                    % Strong prefer connected?
                    % Because timeouts don't count as errors, this does not prune out servers with high timeout count...
                    case [GenTuple || {_, #address_reputation{errors=GenErrors}}=GenTuple <- ChooseFrom0, GenErrors =< MaxErrors] of
                        [] -> referral_choose_address(Referral, OtherAf, Tried, ErrorTolerance);
                        ChooseFrom ->
                            % Spread out requests, or choose one with best latency?
                            {Address, AddressRep} = hd(lists:sort(fun ({_, #address_reputation{latest_latency=L1}}, {_, #address_reputation{latest_latency=L2}}) -> L1 < L2 end, ChooseFrom)),
                            {ok, Address, address_wait_timeout(AddressRep)}
                    end
            end
    end;
referral_choose_address({_, referral, _}=Referral, Af, Tried, ErrorTolerance) ->
    referral_choose_address(referral_add_reputation_info(Referral), Af, Tried, ErrorTolerance).


referral_add_reputation_info({_, referral, NsAddressRrs}=Referral) ->
    AddressRrs = lists:foldl(fun ({_, FunAddressRrs}, FunAcc) -> lists:append(FunAddressRrs, FunAcc) end, [], NsAddressRrs),
    {Referral, referral_with_reputation_info, [{Address, address_reputation(Address)} || {_, _, _, _, Address} <- AddressRrs]}.


% We should timeout fast if we have multiple choices, and timeout slowly
% if it's our only choice. Or slower and try multiple times?
address_wait_timeout(#address_reputation{latest_latency=undefined}) ->
    1000;
address_wait_timeout(#address_reputation{latest_latency=Latest}) ->
    % Use the latest or the average?
    max(1000, Latest * 5 + (rand:uniform(max(Latest div 5, 1)) - 1));
address_wait_timeout(Address) ->
    case address_reputation(Address) of
        unknown -> address_wait_timeout(#address_reputation{address=Address});
        Rep -> address_wait_timeout(Rep)
    end.


address_reputation_fields() ->
    {List, _} = lists:mapfoldl(fun (FunField, Index) -> {{FunField, Index}, Index+1} end, 2, record_info(fields, address_reputation)),
    maps:from_list(List).


init_address_families() ->
    {ok, Addrs} = inet:getifaddrs(),
    init_address_families(Addrs, []).

init_address_families([{_, Args}|Rest], Afs) ->
    init_address_families(Rest, init_address_families_args(Args, Afs));
init_address_families([], Afs) ->
    lists:sort(fun (_, inet6) -> false; (_, _) -> true end, lists:usort(Afs)).

%init_address_families_args([{addr, {127, _, _, _}}|Rest], Afs) ->
%    init_address_families_args(Rest, Afs);
init_address_families_args([{addr, {_, _, _, _}}|Rest], Afs) ->
    init_address_families_args(Rest, [inet|Afs]);
%init_address_families_args([{addr, {0, 0, 0, 0, 0, 0, 0, 1}}|Rest], Afs) ->
%    init_address_families_args(Rest, Afs);
%init_address_families_args([{addr, {16#fe80, 0, 0, 0, _, _, _, _}}|Rest], Afs) ->
%    init_address_families_args(Rest, Afs);
init_address_families_args([{addr, {_, _, _, _, _, _, _, _}}|Rest], Afs) ->
    init_address_families_args(Rest, [inet6|Afs]);
init_address_families_args([_|Rest], Afs) ->
    init_address_families_args(Rest, Afs);
init_address_families_args([], Afs) ->
    Afs.
