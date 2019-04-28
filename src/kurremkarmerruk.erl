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
-module(kurremkarmerruk).

-export([
    resolve_address/1,
    resolve_address/2,
    resolve/1,
    resolve/2
]).

-include_lib("dnslib/include/dnslib.hrl").

resolve_address(Domain) ->
    resolve_address(Domain, [], kurremkarmerruk_recurse_server:available_afs()).

resolve_address(Domain, TriedAddresses) ->
    resolve_address(Domain, TriedAddresses, kurremkarmerruk_recurse_server:available_afs()).

resolve_address(_, _, []) ->
    error;
resolve_address(Domain, TriedAddresses, Afs) ->
    Type = case hd(Afs) of
        inet -> a;
        inet6 -> aaaa
    end,
    case resolve(dnslib:question(Domain, Type, in)) of
        {_, ok, AddressRrs} ->
            % Figure out if any of the addresses are not in the list of disallowed addresses
            case [?RESOURCE_DATA(GenRR) || GenRR <- AddressRrs] -- TriedAddresses of
                [] -> resolve_address(Domain, TriedAddresses, tl(Afs));
                ReturnAddresses -> {ok, ReturnAddresses}
            end;
        _ -> resolve_address(Domain, TriedAddresses, tl(Afs))
    end.

resolve(Query) ->
    resolve(Query, [{namespace, namespace:internal()}]).

resolve(Query, Opts) ->
    % Resilient/background query, increase error/latency tolerance and try harder to arrive at an answer
    case
        case lists:member(no_cache_lookup, Opts) orelse lists:member(bypass_cache, Opts) of
            true -> [];
            false -> kurremkarmerruk_cache:lookup_resource(Query, Opts)
        end
    of
        [] ->
            Namespace = proplists:get_value(namespace, Opts),
            case lists:member(async, Opts) of
                true -> kurremkarmerruk_recurse:resolve(Query, Opts);
                false ->
                    {ok, Answers} = kurremkarmerruk_recurse:resolve(Query, Opts),
                    case lists:member(no_cache_store, Opts) orelse lists:member(bypass_cache, Opts) of
                        true -> ok;
                        false -> kurremkarmerruk_cache:store_interpret_results(Namespace, Answers)
                    end,
                    hd(Answers)
            end;
        {{nodata, Soa}, CnameTrail} -> {Query, nodata, {Soa, CnameTrail}};
        {{name_error, Soa}, CnameTrail} -> {Query, name_error, {Soa, CnameTrail}};
        Results when is_list(Results) -> {Query, ok, Results}
    end.
