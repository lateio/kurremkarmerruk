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
% This file implements the DNS TCP (RFC1034, RFC1035) and TLS protocols.
-module(stream_transfer).

-export([start_link/5,init/5]).

-record(state, {
    length,
    buf= <<>>,
    address,
    port,
    type,
    cbinfo,
    transfer,
    msg,
    id,
    question,
    ref,
    file,
    opts
}).

% What about cases where the zone is extremely large?
% It's not great to just assume that we can handle the zone
% Write resources to disk, then let zone_server to load that?

start_link(Owner, Opts, Msg, Ref, File) ->
    Pid = spawn_link(?MODULE, init, [Owner, Opts, Msg, Ref, File]),
    {ok, Pid}.


init(Owner, Opts0 = #{cbinfo:=Cbinfo={Mod,_,_,_},port:=Port,address:=Address}, Msg, Ref, File) ->
    case is_tuple(Address) of
        false ->
            case kurremkarmerruk:resolve_address(Address, maps:get(tried_addresses, Opts0, [])) of
                {ok, Addresses} ->
                    Opts1 = Opts0#{
                        address => hd(Addresses),
                        orig_address => Address,
                        alt_addresses => tl(Addresses)
                    },
                    init(Owner, Opts1, Msg, Ref, File);
                error ->
                    % Report error to kurremkarmerruk_zone_server, then exit
                    kurremkarmerruk_zone_server:transfer_error(Ref, transport_error, no_usable_address),
                    exit(normal)
                    % What do we do when we have multiple choices? Should prolly save those, in case one or more don't pan out?
                    % Or resolve both and just use the one which arrives first?
                    % Or just provide a kurremkarmerruk:resolve_address function which also handles the choice of
                    % address family transparently. Just use whatever you get back. No point involving every conceivable
                    % module in the confusion about this or that address family, let the kurremkarmerruk_recurse (which actually
                    % connects to other hosts) worry about it.
                    % But how do we preclude some addresses from being returned?
                    %
                    % If we were to resolve from zone, do not resolve from the zone we're updating?
            end;
        true ->
            Timeout = maps:get(timeout, Opts0, timer:seconds(15)),
            case Mod:connect(Address, Port, [{active, true}, binary], Timeout) of
                {ok, Socket} ->
                    [Question] = dnsmsg:questions(Msg),
                    State = #state{
                        address  = Address,
                        port     = Port,
                        cbinfo   = Cbinfo,
                        id       = dnsmsg:id(Msg),
                        msg      = Msg,
                        question = Question,
                        transfer = dnszone:new_transfer(Question),
                        ref      = Ref,
                        file     = File,
                        opts     = Opts0
                    },
                    send_queued(Owner, Socket, State, [Msg]);
                {error, _Reason} ->
                    Opts1 = case Opts0 of
                        #{alt_addresses := [], orig_address := OrigAddress} ->
                            Opts0#{
                                address => OrigAddress,
                                tried_addresses => [Address|maps:get(tried_addresses, Opts0, [])]
                            };
                        #{alt_addresses := AltAddresses} ->
                            Opts0#{
                                address => hd(AltAddresses),
                                alt_addresses => tl(AltAddresses),
                                tried_addresses => [Address|maps:get(tried_addresses, Opts0, [])]
                            }
                    end,
                    init(Owner, Opts1, Msg, Ref, File)
            end
    end.


send_queued(Owner, Socket, State = #state{cbinfo={Mod,_,_,_}}, Queued) ->
    Fn = send_message(Socket, Mod),
    lists:foreach(Fn, lists:reverse(Queued)),
    loop_resolver(Owner, Socket, State).


loop_resolver(Owner, Socket, State = #state{cbinfo={Mod,DataTag,ClosedTag,ErrTag}, ref=Ref}) ->
    receive
        {'EXIT', _, shutdown} ->
            ok = Mod:close(Socket),
            exit(normal);
        {DataTag, Socket, Bin} ->
            case data(Owner, State, Bin) of
                close ->
                    Mod:close(Socket),
                    exit(normal);
                {ok, State1} -> loop_resolver(Owner, Socket, State1);
                {error, _} ->
                    Mod:close(Socket),
                    exit(normal)
            end;
        {ClosedTag, Socket} ->
            % Report error to server (Since at this point we couldn't complete the transfer)
            kurremkarmerruk_zone_server:transfer_error(Ref, transport_error, socket_closed),
            Mod:close(Socket),
            % Should make sure that we got all expected responses
            % This process should never be restarted as it will not contain
            % the information necessary to execute the tasks entrusted to it...
            exit(normal);
        {ErrTag, Socket, Reason} ->
            kurremkarmerruk_zone_server:transfer_error(Ref, transport_error, Reason),
            % Report error to server
            Mod:close(Socket),
            % Should make sure that we got all expected responses
            exit(normal)
    after
        10000 -> % How long should a socket linger?
            % If we have pending requests, count negatively towards the addresses rep
            kurremkarmerruk_zone_server:transfer_error(Ref, transport_error, message_timeout),
            Mod:close(Socket),
            exit(normal)
    end.


data(_, State, <<>>) ->
    {ok, State};
data(Owner, State = #state{length=undefined,buf=Buf0,ref=Ref}, Data) ->
    case <<Buf0/binary, Data/binary>> of
        <<0:16, _/binary>> ->
            % Report error to zone server
            kurremkarmerruk_zone_server:transfer_error(Ref, dns_error, zero_length_message),
            {error, close};
        <<Len:16, Tail/binary>> ->
            data(Owner, State#state{length=Len}, Tail);
        Buf -> data(Owner, State#state{buf=Buf}, <<>>)
    end;
data(Owner, State = #state{length=Length,buf=Buf,id=Id,transfer=Transfer0,ref=Ref,file=File}, Data) ->
    % We could also just compare byte_size() of both
    NewBuf = <<Buf/binary, Data/binary>>,
    case byte_size(NewBuf) >= Length of
        true ->
            <<MsgBin:Length/binary, Tail/binary>> = NewBuf,
            case dnswire:from_binary(MsgBin) of
                {ok, Result, <<>>} ->
                    case dnsmsg:id(Result) of
                        Id ->
                            case dnszone:continue_transfer(Result, Transfer0) of
                                {ok, {zone, Soa, Resources}} ->
                                    ok = dnsfile:write_resources(File, [Soa|Resources], [generic, append]),
                                    io:format("Wrote to file ~p~n", [File]),
                                    % Pass the data to zone server... How does the zone server know which zone we are returning? (namespace...)
                                    % Should we download multiple zones in tandem? Allow that to be capped?
                                    % Close connection, then check zone validity here, don't bother gen_server with that
                                    % Use a lambda or {M,F,A} to store/handle the received records
                                    % Also, why have so many different client modules...
                                    % Generalize and just implement the specifics...
                                    % Ideally even the transport would be generic?
                                    %
                                    % Or implement the dnsclient connection in dnslib?
                                    kurremkarmerruk_zone_server:transfer_complete(Ref),
                                    % Log trailing bytes?
                                    close;
                                {more, Transfer1} ->
                                    {Transfer2, Resources} = dnszone:get_transfer_resources(Transfer1),
                                    ok = dnsfile:write_resources(File, Resources, [generic, append]),
                                    data(Owner, State#state{buf= <<>>, length=undefined, transfer=Transfer2}, Tail);
                                {error, Reason} ->
                                    kurremkarmerruk_zone_server:transfer_error(Ref, dns_error, Reason),
                                    io:format("Error result ~p~n", [Reason]),
                                    % Report error to zone server
                                    close
                            end;
                        _ ->
                            % Report error to zone server
                            kurremkarmerruk_zone_server:transfer_error(Ref, dns_error, unexpected_message_id),
                            {error, close}
                    end;
                {error, Reason, _} ->
                    io:format("Message: ~p~n", [MsgBin]),
                    kurremkarmerruk_zone_server:transfer_error(Ref, dns_error, Reason),
                    {error, close};
                {error, Reason} ->
                    kurremkarmerruk_zone_server:transfer_error(Ref, dns_error, Reason),
                    {error, close}
            end;
        false -> {ok, State#state{buf=NewBuf}}
    end.


send_message(Socket, Transport) ->
    fun (Msg) ->
        {ok, Len, Bin} = dnswire:to_iolist(Msg),
        ok = Transport:send(Socket, [<<Len:16>>,Bin])
    end.
