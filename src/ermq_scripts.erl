%%%-------------------------------------------------------------------
%%% @doc
%%% Lua scripts manager for Ermq.
%%% Handles loading of Lua scripts into Redis, caching SHA digests,
%%% and executing them atomically.
%%%
%%% Features:
%%% 1. Caches SHA digests in ETS.
%%% 2. Recursively resolves "<% include('...') %>" directives common in BullMQ scripts.
%%% 3. Auto-reloads scripts on NOSCRIPT errors.
%%% @end
%%%-------------------------------------------------------------------
-module(ermq_scripts).

%% API
-export([init/0]).
-export([run/4]).
-export([load_command/2]).

-include("ermq.hrl").

-define(ETS_TABLE, ermq_scripts_cache).

%%%===================================================================
%%% API Functions
%%%===================================================================

%% @doc
%% Initializes the ETS table. Call this in your application startup.
init() ->
    try
        ets:new(?ETS_TABLE, [set, public, named_table, {read_concurrency, true}])
    catch
        error:badarg -> ok
    end,
    ok.

%% @doc
%% Executes a Lua script by its specific filename (without .lua extension).
%%
%% Example: ermq_scripts:run(Client, 'addStandardJob-9', [Key1], [Arg1]).
%%
%% @param Client: The Redis connection pid.
%% @param ScriptName: The atom name of the script file (e.g., 'addStandardJob-9').
%% @param Keys: List of Redis keys.
%% @param Args: List of script arguments.
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

%% @doc
%% Reads a script from priv/lua, processes includes, and loads it into Redis.
load_command(Client, ScriptName) when is_atom(ScriptName) ->
    Filename = atom_to_list(ScriptName) ++ ".lua",
    case read_and_process_script(Filename) of
        {ok, Content} ->
            case ermq_redis:q(Client, ["SCRIPT", "LOAD", Content]) of
                {ok, Sha} ->
                    cache_sha(ScriptName, Sha),
                    {ok, Sha};
                {error, Reason} ->
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
        {ok, Result} ->
            {ok, Result};
        {error, <<"NOSCRIPT", _/binary>>} ->
            %% Redis lost the script, reload and retry once
            case load_command(Client, ScriptName) of
                {ok, NewSha} ->
                    NewCmd = ["EVALSHA", NewSha, NumKeys] ++ Keys ++ Args,
                    ermq_redis:q(Client, NewCmd);
                Error ->
                    Error
            end;
        {error, Reason} ->
            {error, Reason}
    end.

get_sha(ScriptName) ->
    case ets:lookup(?ETS_TABLE, ScriptName) of
        [{ScriptName, Sha}] -> Sha;
        [] -> undefined
    end.

cache_sha(ScriptName, Sha) ->
    ets:insert(?ETS_TABLE, {ScriptName, Sha}).

%%--------------------------------------------------------------------
%% Script Pre-processing (Handling Includes)
%%--------------------------------------------------------------------

%% @private
%% Reads the file and recursively resolves includes.
read_and_process_script(Filename) ->
    case get_priv_path(Filename) of
        {ok, Path} ->
            case file:read_file(Path) of
                {ok, Binary} ->
                    Content = binary_to_list(Binary),
                    Processed = process_includes(Content),
                    {ok, list_to_binary(Processed)};
                Error -> Error
            end;
        Error -> Error
    end.

%% @private
%% Locates the file in priv/lua/
get_priv_path(Filename) ->
    case code:priv_dir(ermq) of
        {error, bad_name} ->
            %% Fallback for development if app not started properly
            {error, {app_not_started, "Make sure ermq application is started or path is correct"}};
        PrivDir ->
            {ok, filename:join([PrivDir, "lua", Filename])}
    end.

%% @private
%% Replaces "<% include('path') %>" with file content recursively.
%% BullMQ scripts use the format: <% include('includes/utils') %>
process_includes(Content) ->
    %% Regex to find: <% include('...') %>
    %% We capture the path inside the single quotes.
    Regex = "<%\\s*include\\('([^']*)'\\)\\s*%>",
    
    case re:run(Content, Regex, [global, {capture, [1], list}]) of
        nomatch ->
            Content;
        {match, Matches} ->
            %% Matches is a list of lists: [["includes/utils"], ["includes/destructure"]]
            replace_includes(Content, Matches)
    end.

replace_includes(Content, []) ->
    Content;
replace_includes(Content, [Match | _]) ->
    [IncludePath] = Match,
    %% IncludePath comes as "includes/utils". 
    %% We need to ensure it has .lua extension if missing.
    FullIncludeName = ensure_extension(IncludePath),
    
    Replacement = case read_and_process_script(FullIncludeName) of
        {ok, IncBin} -> binary_to_list(IncBin);
        _ -> "" %% Fail silently or log error? BullMQ usually assumes existence.
    end,
    
    %% Reconstruct the pattern to replace
    Pattern = "<%\\s*include\\('" ++ IncludePath ++ "'\\)\\s*%>",
    
    %% Replace first occurrence (since we iterate matches)
    %% Note: efficient replacement in lists is tricky, using re:replace is easier but returns binary
    NewContent = re:replace(Content, Pattern, Replacement, [{return, list}]),
    
    %% Recurse in case the included file ALSO has includes (unlikely in BullMQ but possible)
    %% Warning: Current simple recursion re-scans the whole file. 
    %% For BullMQ depth, this is acceptable.
    process_includes(NewContent).

ensure_extension(Path) ->
    case filename:extension(Path) of
        ".lua" -> Path;
        _ -> Path ++ ".lua"
    end.