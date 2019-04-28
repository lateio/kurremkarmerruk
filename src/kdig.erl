-module(kdig).
-export([
    resolve/1,
    resolve/2,
    resolve/3
]).

resolve([Head|_] = Domain0) when is_integer(Head) ->
    {_, _, Domain} = dnslib:list_to_domain(Domain0),
    resolve(Domain, a, in);
resolve(Domain) ->
    resolve(Domain, a, in).


resolve([Head|_] = Domain0, Type) when is_integer(Head) ->
    {_, _, Domain} = dnslib:list_to_domain(Domain0),
    resolve(Domain, Type, in);
resolve(Domain, Type) ->
    resolve(Domain, Type, in).


resolve([Head|_] = Domain0, Type, Class) when is_integer(Head) ->
    {_, _, Domain} = dnslib:list_to_domain(Domain0),
    resolve(Domain, Type, Class);
resolve(Domain, Type, Class) ->
    {ok, Answers} = kurremkarmerruk_recurse:resolve({Domain, Type, Class}),
    Answers.
