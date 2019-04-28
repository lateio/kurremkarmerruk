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
-module(recurse_behavior).

-type message_init() ::
    {map(), Questions :: dnslib:question() | [dnslib:question()]} |
    {
        map(),
        Questions :: dnslib:question() | [dnslib:question()],
        Answers   :: dnslib:resource() | [dnslib:resource()]
    } |
    {
        map(),
        Questions :: dnslib:question() | [dnslib:question()],
        Answers   :: dnslib:resource() | [dnslib:resource()],
        Authority :: dnslib:resource() | [dnslib:resource()]
    } |
    {
        map(),
        Questions  :: dnslib:question() | [dnslib:question()],
        Answers    :: dnslib:resource() | [dnslib:resource()],
        Authority  :: dnslib:resource() | [dnslib:resource()],
        Additional :: dnslib:resource() | [dnslib:resource()]
    }.
-type serverspec() :: {'basic' | 'tcp' | 'tls', {inet:ip_address(), inet:port_number()}}.
-type route_question() ::
    {'route_question', message_init(), serverspec(), Timeout, Chain :: [term()]} |
    {'route_question', message_init(), serverspec(), Timeout, Chain :: [term()], TriedServers :: [serverspec()]} |
    {'route_question', message_init(), serverspec(), Timeout, Chain :: [term()], TriedServers :: [serverspec()], TriedQueries :: [dnslib:question()]}.
-type close_question() :: {'close_question', {dnslib:question(), term()}}.
% Allow questions and referrals to be passed to init...
-callback init([dnslib:question()], Config :: term(), [inet | inet6], InitArgs :: term()) ->
    {ok, [route_question() | close_question()], State :: term()}.

-callback reroute([dnslib:question()], Config :: term(), TriedQueriesServers :: term(), [inet | inet6], State0 :: term()) ->
    {ok, [route_question() | close_question()], State1 :: term()}.

-callback response([{dnsmsg:interpret_result(), Chain ::term()}], Response :: dnsmsg:message(), TriedQueriesServers :: term(), Afs :: [atom()], State0 :: term()) ->
    {ok, [route_question() | close_question()], State1 :: term()}.

-callback stop(FinalAnwers :: [dnsmsg:interpret_result()], State :: term()) -> ok.
