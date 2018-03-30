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
-module(recurse_stub).

-behavior(recurse_behavior).
-export([init/4,response/5,reroute/5,stop/2]).

-include_lib("kurremkarmerruk/include/recurse.hrl").


init(Questions, Servers, UseAf, _) ->
    Referral = make_proto_referral(Servers),
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
    Tuple = case kurremkarmerruk_recurse:referral_choose_address(Referral, UseAf) of
        {error, no_suitable_address} -> {close_question, {Question, {error, no_suitable_address}}};
        {ok, Address} ->
            ServerSpec = address_to_server_spec(Address, Servers),
            {route_question, {#{recursion_desired => true}, Question}, ServerSpec, [Referral]}
    end,
    init_questions(Rest, ProtoReferral, Servers, UseAf, [Tuple|Acc]).


reroute_questions([], _, _, _, Acc) ->
    Acc;
reroute_questions([{Question, [Referral]}|Rest], Servers, Tried, UseAf, Acc) ->
    case kurremkarmerruk_recurse:referral_choose_address(Referral, UseAf, Tried) of
        {ok, Address} ->
            ServerSpec = address_to_server_spec(Address, Servers),
            Tuple = {route_question, {#{recursion_desired => true}, Question}, ServerSpec, [Referral], Tried},
            reroute_questions(Rest, Servers, Tried, UseAf, [Tuple|Acc]);
        {error, no_suitable_address} ->
            Tuple = {close_question, {Question, {error, no_suitable_address}}},
            reroute_questions(Rest, Servers, Tried, UseAf, [Tuple|Acc])
    end.


make_proto_referral(Servers) ->
    % We could drop all but tls servers from the list.
    % How do we prefer tls to others?
    Fn = fun
        ({{_, {{_, _, _, _}=Address, _}}, _}) -> {nil, a, in, 0, Address};
        ({{_, {{_, _, _, _, _, _, _, _}=Address, _}}, _}) -> {nil, aaaa, in, 0, Address}
    end,
    {nil, referral, [{nil, lists:map(Fn, Servers)}]}.


address_to_server_spec(Address, [Server|Rest]) ->
    case Server of
        {{_, {Address, _}}=ServerSpec, _} -> ServerSpec;
        _ -> address_to_server_spec(Address, Rest)
    end.
