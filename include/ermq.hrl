%%%-------------------------------------------------------------------
%%% @author Ermq Porter
%%% @doc
%%% Common definitions, macros and records for the Ermq application.
%%% @end
%%%-------------------------------------------------------------------

%% Default prefix for all keys in Redis.
-define(DEFAULT_PREFIX, <<"ermq">>).

%% Standard job options map default structure
-define(DEFAULT_JOB_OPTS, #{
    attempts => 1,
    delay => 0,
    timestamp => os:system_time(millisecond)
}).