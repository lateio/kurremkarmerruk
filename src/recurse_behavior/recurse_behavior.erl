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
    {'route_question', message_init(), serverspec(), Chain :: [term()]} |
    {'route_question', message_init(), serverspec(), Chain :: [term()], TriedServers :: [serverspec()]} |
    {'route_question', message_init(), serverspec(), Chain :: [term()], TriedServers :: [serverspec()], TriedQueries :: [dnslib:question()]}.
-type close_question() :: {'close_question', {dnslib:question(), term()}}.
% Allow questions and referrals to be passed to init...
-callback init([dnslib:question()], Config :: term(), [inet | inet6], InitArgs :: term()) ->
    {ok, [route_question() | close_question()], State :: term()}.

-callback reroute([dnslib:question()], Config :: term(), TriedQueriesServers :: term(), [inet | inet6], State0 :: term()) ->
    {ok, [route_question() | close_question()], State1 :: term()}.

-callback response([{dnsmsg:interpret_result(), Chain ::term()}], Response :: dnsmsg:message(), TriedQueriesServers :: term(), Afs :: [atom()], State0 :: term()) ->
    {ok, [route_question() | close_question()], State1 :: term()}.

-callback stop(FinalAnwers :: [dnsmsg:interpret_result()], State :: term()) -> ok.
