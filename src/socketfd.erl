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
% This module - and the NIFs associated with it - enable the allocation
% and binding of raw socket fds, which can later on be used with gen_udp and such.
%
% Note: most NIFs can return POSIX error details. Typically part of the reason
%       for using Erlang is presumably to escape the need to painstakingly check
%       for every error. Thus when using this module, please stick to the rule
%       of minimal local error handling, even though detailed error reasons
%       are available.
-module(socketfd).
-export([
    open/3,
    get/1,
    dup/1,
    close/1
]).

-on_load(load_nifs/0).

-opaque socketfd() :: identifier().
-type socket_type() ::
    'tcp'       |
    'stream'    |
    'udp'       |
    'datagram'.
-type socket_family() :: 'inet' | 'inet6'.


-export_type([socketfd/0]).

load_nifs() ->
    App = kurremkarmerruk,
    case code:priv_dir(App) of
        {error, bad_name} -> error(bad_appname);
        Dir -> erlang:load_nif(filename:join(Dir, App), 0)
    end.


-spec open(socket_type(), socket_family() | inet:ip_address(), inet:port_number()) ->
    {'ok', socketfd()} | {'error', term()}.
open(_, _, _) -> error.


-spec dup(socketfd()) -> {'ok', socketfd()}.
dup(_) -> error.


-spec get(socketfd()) -> integer().
get(_) -> error.


-spec close(socketfd()) -> 'ok'.
close(_) -> error.
