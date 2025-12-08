%%%-------------------------------------------------------------------
%%% Standard errors definitions for Ermq.
%%% Corresponds to src/classes/errors.ts.
%%% Provides helper functions to generate standard error tuples.
%%%-------------------------------------------------------------------
-module(ermq_errors).

%% API
-export([new/1, new/2]).
-export([format_error/1]).

%% Types
-export_type([error_reason/0]).

-type error_reason() :: 
      job_not_found 
    | job_not_moved 
    | job_not_finished 
    | lock_error 
    | missing_process_handler 
    | connection_closed.

%%%===================================================================
%%% API Functions
%%%===================================================================

%% Returns a standard error tuple with a default message derived from the reason.
%%
%% Example:
%%   ermq_errors:new(job_not_found).
%%   => {error, job_not_found, "Job not found"}
-spec new(error_reason()) -> {error, error_reason(), string()}.
new(Reason) ->
    {error, Reason, get_default_msg(Reason)}.

%% Returns a standard error tuple with a custom message or context.
%% Useful when you want to override the default text or add details.
%%
%% Example:
%%   ermq_errors:new(job_not_found, "Job 123 missing").
%%   => {error, job_not_found, "Job 123 missing"}
-spec new(error_reason(), string() | binary()) -> {error, error_reason(), string() | binary()}.
new(Reason, Msg) ->
    {error, Reason, Msg}.

%% Formats an error reason atom into a human-readable string.
%% Used internally by new/1 but exposed for logging or UI purposes.
-spec format_error(error_reason() | term()) -> string().
format_error(job_not_found)           -> "Job not found";
format_error(job_not_moved)           -> "Job could not be moved to new state";
format_error(job_not_finished)        -> "Job is not in a finished state";
format_error(lock_error)              -> "Could not acquire lock";
format_error(missing_process_handler) -> "No process handler attached";
format_error(connection_closed)       -> "Redis connection is closed";
format_error(Other)                   -> 
    %% io_lib:format returns a deep list (iolist), so we must flatten it
    %% to match the spec string().
    lists:flatten(io_lib:format("Unknown error: ~p", [Other])).

%%%===================================================================
%%% Internal Functions
%%%===================================================================

%% Helper to fetch default message for new/1
get_default_msg(Reason) ->
    format_error(Reason).