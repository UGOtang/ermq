%%%-------------------------------------------------------------------
%%% @doc
%%% Standard errors definitions for Ermq.
%%% Corresponds to src/classes/errors.ts.
%%% Provides helper functions to generate standard error tuples.
%%% @end
%%%-------------------------------------------------------------------
-module(ermq_errors).

%% API
-export([new/1, new/2]).
-export([format_error/1]).

%% Error Types (Atoms)
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

%% @doc
%% Returns a standard error tuple with a default message.
%% Example: ermq_errors:new(job_not_found) -> {error, job_not_found, "Job not found"}
-spec new(error_reason()) -> {error, error_reason(), string()}.
new(Reason) ->
    {error, Reason, get_default_msg(Reason)}.

%% @doc
%% Returns a standard error tuple with a custom message/context.
%% Example: ermq_errors:new(job_not_found, "Job 123 missing")
-spec new(error_reason(), string() | binary()) -> {error, error_reason(), string() | binary()}.
new(Reason, Msg) ->
    {error, Reason, Msg}.

%% @doc
%% Formats an error reason into a human-readable string.
format_error(job_not_found) -> "Job not found";
format_error(job_not_moved) -> "Job could not be moved to new state";
format_error(job_not_finished) -> "Job is not in a finished state";
format_error(lock_error) -> "Could not acquire lock";
format_error(missing_process_handler) -> "No process handler attached";
format_error(connection_closed) -> "Redis connection is closed";
format_error(Other) -> io_lib:format("Unknown error: ~p", [Other]).

%%%===================================================================
%%% Internal Functions
%%%===================================================================

get_default_msg(Reason) ->
    format_error(Reason).