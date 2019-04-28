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
-module(recurse_stub).

-behavior(recurse_behavior).
-export([init/4,response/5,reroute/5,stop/2]).

-include_lib("dnslib/include/dnslib.hrl").
-include_lib("kurremkarmerruk/include/recurse.hrl").

-import(kurremkarmerruk_recurse_address_reputation, [
    referral_choose_address/2,
    referral_choose_address/3
]).

-import(kurremkarmerruk_recurse_resolve, [
    route_question/5,
    route_question/6
]).


init([Question] = Questions, Servers, UseAf, _) ->
    Referral = make_proto_referral(Question, UseAf, Servers),
    {ok, init_questions(Questions, Referral, Servers, UseAf, []), nil}.

reroute(Questions, Servers, Tried, UseAf, nil) ->
    {ok, reroute_questions(Questions, Servers, Tried, UseAf, []), nil}.

response(Answers, _, _, _, nil) ->
    {ok, close_questions(Answers, []), nil}.

stop(_, _) -> ok.


close_questions([], Acc) ->
    Acc;
close_questions([{Answer, _}|Rest], Acc) ->
    close_questions(Rest, [{close_question, Answer}|Acc]).


init_questions([], _, _, _, Acc) ->
    Acc;
init_questions([Entry|Rest], {_, referral, NsAddressRrs}=ProtoReferral, Servers, UseAf, Acc) ->
    case Entry of
        {Question, _, _} when is_tuple(Question) -> ok;
        Question -> ok
    end,
    Referral = {Question, referral, NsAddressRrs},
    Tuple = case referral_choose_address(Referral, UseAf) of
        {error, no_suitable_address} -> {close_question, {Question, {error, no_suitable_address}}};
        {ok, Address, Timeout} ->
            ServerSpec = address_to_server_spec(Address, Servers),
            route_question(#{recursion_desired => true}, Question, ServerSpec, Timeout, [Referral])
    end,
    init_questions(Rest, ProtoReferral, Servers, UseAf, [Tuple|Acc]).


reroute_questions([], _, _, _, Acc) ->
    Acc;
reroute_questions([{Question, [Referral]}|Rest], Servers, Tried, UseAf, Acc) ->
    case referral_choose_address(Referral, UseAf, Tried) of
        {ok, Address, Timeout} ->
            ServerSpec = address_to_server_spec(Address, Servers),
            Tuple = route_question(#{recursion_desired => true}, Question, ServerSpec, Timeout, [Referral], Tried),
            reroute_questions(Rest, Servers, Tried, UseAf, [Tuple|Acc]);
        {error, no_suitable_address} ->
            Tuple = {close_question, {Question, {error, no_suitable_address}}},
            reroute_questions(Rest, Servers, Tried, UseAf, [Tuple|Acc])
    end.


make_proto_referral(Query, UseAf, Servers) ->
    % We could drop all but tls servers from the list.
    % How do we prefer tls to others?
    Question = case Query of
        {CaseQuestion, _, _} when is_tuple(CaseQuestion) -> CaseQuestion;
        Query -> Query
    end,
    Fn = fun
        ({{_, {{_, _, _, _}=Address, _}}, _}) -> {nil, a, in, 0, Address};
        ({{_, {{_, _, _, _, _, _, _, _}=Address, _}}, _}) -> {nil, aaaa, in, 0, Address};
        ({{_, {Domain, _}}, _}) ->
            Type = case hd(UseAf) of
                inet -> a;
                inet6 -> aaaa
            end,
            case kurremkarmerruk_cache:lookup_resource(dnslib:question(Domain, Type, in)) of
                [AddressRR|_] -> {nil, a, in, 0, ?RESOURCE_DATA(AddressRR)};
                [] ->
                    case kurremkarmerruk_recurse:resolve(dnslib:question(Domain, Type, in)) of
                        {ok, AAnswers = [{_, ok, [AddressRR|_]}]} ->
                            kurremkarmerruk_cache:store_interpret_results(namespace:internal(), AAnswers),
                            {nil, a, in, 0, ?RESOURCE_DATA(AddressRR)}
                        % _ -> How should we handle errors?
                    end
            end
    end,
    {Question, referral, [{{?QUESTION_DOMAIN(Question), ns, in, 0, nil}, lists:map(Fn, Servers)}]}.


address_to_server_spec(Address, []) ->
    {basic, {Address, 53}};
    %{tls, {Address, 853}};...
address_to_server_spec(Address, [Server|Rest]) ->
    case Server of
        {{_, {Address, _}}=ServerSpec, _} -> ServerSpec;
        % When we use a domain, we can't use this method to match the serverspec...
        _ -> address_to_server_spec(Address, Rest)
    end.
