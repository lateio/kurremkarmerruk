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
-module(kurremkarmerruk_zone_fs_watcher).

-behavior(gen_server).
-export([init/1,code_change/3,terminate/2,handle_call/3,handle_cast/2,handle_info/2]).
-export([start_link/0]).

-export([
    watch_file/1,
    watch_dir/1
]).

-include_lib("dnslib/include/dnslib.hrl").
-include_lib("dnslib/include/dnsfile.hrl").

-record(file, {
    id      :: binary(),
    pids=[] :: [pid()],
    path    :: string()
}).

-record(dir, {
    id           :: binary(),
    pids=[]      :: [pid()] | 'undefined',
    path         :: string(),
    opts=[],
    related=[]   :: [binary()],
    files=[]     :: [string()],
    child_ids=[] :: [binary()],
    role=target  :: 'target' | 'parent' | 'child'
}).

-record(server_state, {
    port        :: port(),
    targets=#{} :: #{binary() => #file{} | #dir{}}
}).


start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


init([]) ->
    % Init storage here
    % Where will we keep zone files?
    process_flag(trap_exit, true),
    {ok, Port} = watch_drv:open(),
    {ok, #server_state{port=Port}}.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


terminate(_, #server_state{port=Port}) ->
    if
        Port =/= undefined -> true = port_close(Port); % Avoid ** exception error: driver_unloaded which occurs if the port is not "correctly" terminated
        true -> ok
    end,
    ok.


handle_call({watch_file, Pid, Path}, _, State = #server_state{port=Port,targets=Targets0}) ->
    Id = path2id(Path),
    [Root|Rest] = filename:split(Path),
    Targets1 = track_hierarchy(Port, Root, Rest, path2id_hier(Path, []), Targets0),
    ok = watch_drv:watch(Port, Path, Id, [write, delete, rename]),
    #file{pids=Pids} = File = maps:get(Id, Targets0, #file{id=Id, path=unicode:characters_to_binary(Path, unicode, file:native_name_encoding())}), % Should prolly have an idea of what we were supposed to track...
    % Also, what if this was not the first time that file is tracked?
    {reply, ok, State#server_state{targets=Targets1#{Id => File#file{pids=lists:usort([Pid|Pids])}}}};
handle_call({watch_dir, Pid, Path, Opts}, _, State = #server_state{port=Port,targets=Targets0}) ->
    {ok, Targets1} = watch_dir(Pid, Path, Opts, Port, Targets0),
    {reply, ok, State#server_state{targets=Targets1}};
handle_call(_, _, State) ->
    {noreply, State}.


handle_cast(_, State) ->
    {noreply, State}.


handle_info({watch, Port, Id, Events}=Msg, State = #server_state{port=Port,targets=Targets0}) ->
    % On file delete we need to check if we're already monitoring the parent directory
    % of file. If we're not, we should get on with that, in order to make sure that
    % that we don't miss should the file be created again
    %
    % If someone deletes the directory which contained our file, the delete
    % propagates to our file eventually. Thus, on delete we need to start searching
    % for the nearest existing parent folder and start tracking there
    io:format("Fs watcher server ~p~n", [Msg]),
    Targets1 = handle_watch_events(Events, Port, Id, Targets0),
    {noreply, State#server_state{targets=Targets1}};
handle_info({'EXIT', Port, driver_unloaded}, State = #server_state{port=Port}) ->
    {noreply, State#server_state{port=undefined}}.


handle_watch_events([], _, _, Targets) ->
    Targets;
handle_watch_events([Event|Rest], Port, Id, Targets0) ->
    Targets1 = case maps:get(Id, Targets0, undefined) of
        % Should we enforce some moment of cooldown for write/create?
        #file{pids=Pids, path=Path} ->
            Fn = case Event of
                % Need to delay write messages a tad
                write  -> fun (FunPid) -> FunPid ! {file_event, Path, change}, ok end;
                delete -> fun (FunPid) -> FunPid ! {file_event, Path, delete}, ok end
            end,
            [Fn(GenPid) || GenPid <- Pids],
            Targets0; % On delete we need to remove the target and stop tracking it
        #dir{role=parent} ->
            case Event of
                delete -> error(Targets0);
                rename -> error(Targets0);
                _ -> Targets0
            end;
            % If the event was a write, check that the files we care about are still there
            % On delete/rename, there's no such chance
            %io:format("Parent event ~p~n", [Events]),
            %Targets0;
        #dir{role=Role, files=PrevFiles, pids=Pids, path=Path} = Dir ->
            case Event of
                write ->
                    NewFiles = case file:list_dir(Path) of
                        {ok, CaseFiles} -> CaseFiles;
                        {error, _} -> []
                    end,
                    case NewFiles -- PrevFiles of
                        [] -> Targets0#{Id => Dir#dir{files=NewFiles}}; % No new files, and the deletes anyone cares about are gonna be handled elsewhere
                        CreatedFilesDirs ->
                            recurse_new(
                                [GenFile || GenFile <- CreatedFilesDirs, hd(GenFile) =/= $.],
                                Dir#dir{files=NewFiles},
                                Port,
                                Targets0
                            )
                    end;
                %[rename] -> What if the dir was renamed to something that's still under under watch?
                delete ->
                    Fn = fun (FunId, FunTargets) ->
                        case FunTargets of
                            #{FunId := #file{pids=FunPids, path=FunPath}} ->
                                lists:map(fun (FunFunPid) -> FunFunPid ! {file_event, FunPath, deleted}, ok end, FunPids),
                                ok = watch_drv:stop_watching(Port, FunId),
                                maps:remove(FunId, FunTargets);
                            #{FunId := #dir{role=FunRole, path=FunPath}} ->
                                ok = watch_drv:stop_watching(Port, FunId),
                                maps:remove(FunId, FunTargets);
                            #{} -> FunTargets
                        end
                    end,
                    ok = watch_drv:stop_watching(Port, Id),
                    lists:foldl(Fn, maps:remove(Id, Targets0), Dir#dir.related)
            end
    end,
    handle_watch_events(Rest, Port, Id, Targets1).


recurse_new([], Dir = #dir{id=Id}, _, Targets) ->
    Targets#{Id => Dir};
recurse_new([FileDir|Rest], Dir = #dir{path=Path0, pids=Pids}, Port, Targets0) ->
    % If some of the created objects were directories (and we were configured for to act recursively),
    % Should prolly skip those messages and just get to tracking them
    % also. what if there's a deeper hierarchy already?
    % Need to recurse and recurse...
    %io:format("recurse_new Pids ~p~n", [Pids]),
    % Also, some of these files might well be dotfiles...
    Path = filename:join(Path0, FileDir),
    IsDir = filelib:is_dir(Path),
    IsFile = filelib:is_regular(Path),
    if
        IsDir ->
            Fn = fun (FunPid, FunTargets0) ->
                FunId = path2id(Path),
                [FunRoot|FunRest] = filename:split(Path),
                FunTargets1 = track_hierarchy(Port, FunRoot, FunRest, path2id_hier(Path, []), FunTargets0),
                %io:format("Tracking dir hierarchy ~p~n", [Path]),
                ok = watch_drv:watch(Port, Path, FunId, [write, delete, rename]),
                {ok, Files} = file:list_dir(Path),
                #dir{pids=FunPids} = FunDir0 = maps:get(FunId, FunTargets0, #dir{id=FunId, path=unicode:characters_to_binary(Path, unicode, file:native_name_encoding()), role=child}),
                FunDir1 = FunDir0#dir{pids=lists:usort([FunPid|Pids]), files=Files},
                %io:format("Reply ~p ~p~n", [FunDir1, Files]),
                FunTargets2 = FunTargets1#{FunId => FunDir1},
                recurse_new([GenFile || GenFile <- Files, hd(GenFile) =/= $.], FunDir1, Port, FunTargets2)
            end,
            recurse_new(Rest, Dir, Port, lists:foldl(Fn, Targets0, Pids));
        IsFile ->
            Fn = fun (FunPid) ->
                % Maybe delay this too, so that any files added as a group have a chance to show up together?
                FunPid ! {dir_event, Path, created},
                ok
            end,
            [Fn(GenPid) || GenPid <- Pids],
            %io:format("Add file ~p~n", [Path]),
            recurse_new(Rest, Dir, Port, Targets0);
        true -> recurse_new(Rest, Dir, Port, Targets0)
    end.


% Whats an Erlang-like way/wording of file watching?
watch_file(Path0) ->
    % Allow the caller to pass a term which will then be sent in addition to events...?
    % Should we check that it's actually a file?
    Path = filename:absname(Path0),
    case filelib:is_regular(Path) of
        true -> gen_server:call(?MODULE, {watch_file, self(), Path});
        false -> {error, not_a_file}
    end.


% How can/should we monitor a directory?
% Keep track of which files we've already reported,
% which files have gone missing ... Or only report new files.
% Then as the server checks the newly created files,
% it'll tell us to specifically watch this or that file,
% leaving us to not worry about file deletions.
% Also
% {dir_event, Filename, created}
watch_dir(Path) ->
    watch_dir(Path, []).

watch_dir(Path0, Opts) ->
    Path = filename:absname(Path0),
    case filelib:is_dir(Path) of
        true -> gen_server:call(?MODULE, {watch_dir, self(), Path, Opts});
        false -> {error, not_a_dir}
    end.

watch_dir(Pid, Path, Opts, Port, Targets0) ->
    Id = path2id(Path),
    [Root|Rest] = filename:split(Path),
    Targets1 = track_hierarchy(Port, Root, Rest, path2id_hier(Path, []), Targets0),
    ok = watch_drv:watch(Port, Path, Id, [write, delete, rename]),
    {ok, Files} = file:list_dir(Path),
    #dir{pids=Pids} = Dir = maps:get(Id, Targets0, #dir{id=Id, path=unicode:characters_to_binary(Path, unicode, file:native_name_encoding()), role=target}),
    {ok, Targets1#{Id => Dir#dir{pids=[Pid|Pids], files=Files}}}.


track_hierarchy(Port, Path, Components, Ids, Targets0) ->
    Id = hd(Ids),
    Targets1 = case maps:get(Id, Targets0, undefined) of
        undefined ->
            Dir = #dir{id=Id, path=unicode:characters_to_binary(Path, unicode, file:native_name_encoding()), role=parent, files=[hd(Components)], related=tl(Ids)},
            ok = watch_drv:watch(Port, Path, Id, [write, delete, rename]),
            Targets0#{Id => Dir};
        #dir{related=OldRel} = Dir ->
            AddRel = tl(Ids) -- OldRel,
            Targets0#{Id => Dir#dir{related=AddRel ++ OldRel}}
    end,
    case Components of
        [_] -> Targets1;
        [Next|Rest] -> track_hierarchy(Port, filename:join(Path, Next), Rest, tl(Ids), Targets1)
    end.


path2id(Path) when is_list(Path) ->
    path2id(unicode:characters_to_binary(Path, unicode, file:native_name_encoding()));
path2id(Path) when is_binary(Path) ->
    <<(erlang:phash2(Path, 16#FFFFFFFF)):32>>.


path2id_hier("/", Acc) ->
    [path2id("/")|Acc];
path2id_hier(<<"/">>, Acc) ->
    [path2id(<<"/">>)|Acc];
path2id_hier(Path, Acc) ->
    path2id_hier(filename:dirname(Path), [path2id(Path)|Acc]).
