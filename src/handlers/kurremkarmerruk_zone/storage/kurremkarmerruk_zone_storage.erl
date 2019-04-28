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
-module(kurremkarmerruk_zone_storage).

-export([
    store_zone/3,
    get_resource/3,
    get_zone/4,
    authoritative_for/4,
    delete_zone/4, % Disabling a zone without deleting it?
    update_zone/3
]).

-include_lib("dnslib/include/dnslib.hrl").

-callback init(StorageName :: atom(), Opts :: list()) -> {Module :: atom(), State :: term()}.

% persistent() -> ...
% synchronous

% Compared to cache
% Needs complete information of zone. Maybe even multiple versions of a zone
% Resources don't timeout
% Zones also have a source...
% Need to have an index of zones...
% get_zones() -> [dnslib:resource(soa)]
% Or index of transfers?

% Add zone... -> Find borders, then store resources

-callback add_zone(State :: term(), Soa :: dnslib:resource(), Edges :: [dnslib:domain()], NsID :: term()) -> ok.

-callback add_resource(State :: term(), Resource :: dnslib:resource() | [dnslib:resource()], NsID :: term()) -> ok.

-callback get_resource(State :: term(), Question :: dnslib:question(), NsId :: term())
    -> {ok, Authoritative :: boolean(), Resources :: [dnslib:resource()]}
     | {referral, NsResources :: [dnslib:resource()]}
     | {nodata, Soa :: dnslib:resource()}
     | {name_error, Soa :: dnslib:resource()}
     | not_in_zone.

-callback get_zone(Apex :: dnslib:domain()) -> {ok, Zone :: [dnslib:resource()]} | unknown.

-callback delete_zone(State :: term(), Apex :: dnslib:domain(), Class :: dnsclass:class(), NSID :: term()) -> ok.

-callback authoritative_for(State :: term(), Domain :: dnslib:domain(), Class :: dnsclass:class(), NSID :: term()) -> boolean().

%-callback delete_resource(State :: term(), dnslib:question(), dnslib:resource(), NSID :: term()) -> ok.

%-callback update_zone(State, Changes, NSID)


% Load from a file?
store_zone(Rrs, {Module, State}, NSID) when not is_list(NSID) ->
    case dnszone:is_valid(Rrs) of
        true ->
            case [GenRR || GenRR <- Rrs, ?RESOURCE_TYPE(GenRR) =:= soa] of
                [Soa] ->
                    Edges = [GenRR || GenRR <- Rrs, ?RESOURCE_TYPE(GenRR) =:= ns],
                    ok = Module:add_zone(State, Soa, Edges, NSID),
                    ok = Module:add_resource(State, Rrs, NSID)
            end;
        {false, Reason} -> {error, Reason}
    end;
store_zone(Rrs, Tuple, [NSID|Rest]) ->
    ok = store_zone(Rrs, Tuple, NSID),
    store_zone(Rrs, Tuple, Rest);
store_zone(_, _, []) ->
    ok.


% This rather terribly wasteful, should require a update_zone callback
% in storage modules in the future
update_zone(Rrs, ModState, NSID) when not is_list(NSID) ->
    case [GenRR || GenRR <- Rrs, ?RESOURCE_TYPE(GenRR) =:= soa] of
        [Soa] ->
            ok = delete_zone(?RESOURCE_DOMAIN(Soa), ?RESOURCE_CLASS(Soa), ModState, NSID),
            ok = store_zone(Rrs, ModState, NSID);
        [] -> {error, no_soa};
        _ -> {error, multiple_soas}
    end;
update_zone(Rrs, Tuple, [NSID|Rest]) ->
    ok = update_zone(Rrs, Tuple, NSID),
    update_zone(Rrs, Tuple, Rest);
update_zone(_, _, []) ->
    ok.


get_resource(Question, {Module, State}, NSID) ->
    Module:get_resource(State, Question, NSID).


% Don't store zones for zone transfers in storage...
get_zone(Apex, Class, {Module, State}, NSID) ->
    Module:get_zone(State, Apex, Class, NSID).


delete_zone(Apex, Class, {Module, State}, NSID) when not is_list(NSID) ->
    Module:delete_zone(State, Apex, Class, NSID);
delete_zone(Apex, Class, Tuple, [NSID|Rest]) ->
    ok = delete_zone(Apex, Class, Tuple, NSID),
    delete_zone(Apex, Class, Tuple, Rest);
delete_zone(_, _, _, []) ->
    ok.


authoritative_for(Domain, Class, {Module, State}, NSID) ->
    Module:authoritative_for(State, Domain, Class, NSID).
