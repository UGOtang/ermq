%%%-------------------------------------------------------------------
%%% Lua scripts manager for Ermq.
%%% Handles loading of Lua scripts into Redis, caching SHA digests,
%%% and executing them atomically.
%%%
%%% Features:
%%% 1. Caches SHA digests in ETS.
%%% 2. Recursively resolves "--- @include" directives.
%%% 3. Auto-reloads scripts on NOSCRIPT errors.
%%%-------------------------------------------------------------------
-module(ermq_scripts).

%% API
-export([init/0, run/4, load_command/2]).

-define(ETS_TABLE, ermq_scripts_cache).

%%%===================================================================
%%% API Functions
%%%===================================================================

%% Initializes the ETS table. Call this during app startup.
init() ->
    try ets:new(?ETS_TABLE, [set, public, named_table, {read_concurrency, true}])
    catch error:badarg -> ok end,
    ok.

%% Executes a script. Tries cached SHA first, then loads if missing.
run(Client, ScriptName, Keys, Args) ->
    case get_sha(ScriptName) of
        undefined ->
            case load_command(Client, ScriptName) of
                {ok, Sha} -> eval_sha(Client, ScriptName, Sha, Keys, Args);
                Error -> Error
            end;
        Sha ->
            eval_sha(Client, ScriptName, Sha, Keys, Args)
    end.

%% Reads script from file, processes includes, loads into Redis.
load_command(Client, ScriptName) when is_atom(ScriptName) ->
    Filename = atom_to_list(ScriptName) ++ ".lua",
    case read_and_process_script(Filename) of
        {ok, Content} ->
            case ermq_redis:q(Client, ["SCRIPT", "LOAD", Content]) of
                {ok, Sha} ->
                    cache_sha(ScriptName, Sha),
                    {ok, Sha};
                {error, Reason} ->
                    logger:error("Script Load Error for ~p: ~p", [ScriptName, Reason]),
                    {error, {script_load_error, Reason}}
            end;
        {error, Reason} ->
            {error, {file_read_error, Reason}}
    end.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

eval_sha(Client, ScriptName, Sha, Keys, Args) ->
    NumKeys = integer_to_list(length(Keys)),
    Cmd = ["EVALSHA", Sha, NumKeys] ++ Keys ++ Args,
    case ermq_redis:q(Client, Cmd) of
        {ok, Res} -> {ok, Res};
        {error, <<"NOSCRIPT", _/binary>>} ->
            %% Redis lost the script (restart/flush). Reload and retry.
            case load_command(Client, ScriptName) of
                {ok, NewSha} ->
                    NewCmd = ["EVALSHA", NewSha, NumKeys] ++ Keys ++ Args,
                    ermq_redis:q(Client, NewCmd);
                Err -> Err
            end;
        Err -> Err
    end.

get_sha(Script) ->
    case ets:lookup(?ETS_TABLE, Script) of
        [{Script, Sha}] -> Sha; [] -> undefined
    end.

cache_sha(Script, Sha) -> ets:insert(?ETS_TABLE, {Script, Sha}).

%%--------------------------------------------------------------------
%% Script Pre-processing
%%--------------------------------------------------------------------

read_and_process_script(Filename) ->
    case find_script_path(Filename) of
        {ok, Path} ->
            case file:read_file(Path) of
                {ok, Binary} ->
                    Content = binary_to_list(Binary),
                    try
                        Processed = process_includes(Content),
                        {ok, list_to_binary(Processed)}
                    catch throw:Err -> {error, Err} end;
                {error, R} -> {error, R}
            end;
        E -> E
    end.

%% Locates the file in priv/lua or priv/lua/includes.
find_script_path(Filename) ->
    case code:priv_dir(ermq) of
        {error, _} -> {error, {app_not_started, "ermq app not started"}};
        PrivDir ->
            %% 1. Try direct path: priv/lua/<Filename>
            PathRoot = filename:join([PrivDir, "lua", Filename]),
            case filelib:is_file(PathRoot) of
                true -> {ok, PathRoot};
                false ->
                    %% 2. Fallback: Try priv/lua/includes/<Filename>
                    PathIncludes = filename:join([PrivDir, "lua", "includes", Filename]),
                    case filelib:is_file(PathIncludes) of
                        true -> {ok, PathIncludes};
                        false -> {error, enoent} 
                    end
            end
    end.

process_includes(Content) ->
    %% Regex: --- @include "path/to/file"
    Regex = "--- @include \"([^\"]+)\"",
    
    case re:run(Content, Regex, [global, {capture, all, list}]) of
        nomatch ->
            process_includes_old(Content);
        {match, Matches} ->
            replace_includes(Content, Matches, Regex)
    end.

process_includes_old(Content) ->
    %% Regex: <% include('path') %>
    Regex = "<%\\s*include\\((?:'([^']*)'|\"([^\"]*)\")\\)\\s*%>",
    case re:run(Content, Regex, [global, {capture, all, list}]) of
        nomatch -> Content;
        {match, Matches} -> replace_includes(Content, Matches, Regex)
    end.

replace_includes(Content, [], _) -> Content;
replace_includes(Content, [Match | _], Regex) ->
    [FullMatch | Captures] = Match,
    Path = pick_path(Captures),
    FullIncludeName = ensure_extension(Path),
    
    Replacement = case read_and_process_script(FullIncludeName) of
        {ok, IncBin} -> binary_to_list(IncBin);
        {error, R} -> 
            logger:error("Missing include: ~p (~p)", [FullIncludeName, R]),
            throw({missing_include, FullIncludeName, R})
    end,
    
    IoData = string:replace(Content, FullMatch, Replacement),
    NewContent = unicode:characters_to_list(IoData),
    
    %% Recurse to handle nested includes
    case Regex of
        "--- @include" ++ _ -> process_includes(NewContent);
        _ -> process_includes_old(NewContent)
    end.

pick_path([P]) -> P;
pick_path([P, []]) -> P;
pick_path([[], P]) -> P.

ensure_extension(Path) ->
    case filename:extension(Path) of
        ".lua" -> Path;
        _ -> Path ++ ".lua"
    end.