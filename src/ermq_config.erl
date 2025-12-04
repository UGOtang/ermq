%%%-------------------------------------------------------------------
%%% @doc
%%% Configuration management module.
%%% Handles merging of user options with defaults for Queues and Workers.
%%% @end
%%%-------------------------------------------------------------------
-module(ermq_config).

%% API
-export([new_queue_opts/1, new_worker_opts/1]).
-export([get_redis_opts/1]).

-include("include/ermq.hrl").

%%%===================================================================
%%% API Functions
%%%===================================================================

%% @doc
%% Merges user provided queue options with defaults.
%% @param Opts: A map containing queue configuration.
%% @return A map with complete queue options.
new_queue_opts(Opts) when is_map(Opts) ->
    Defaults = #{
        prefix => ?DEFAULT_PREFIX,
        connection => #{}, %% Default redis connection options
        defaultJobOptions => #{}
    },
    maps:merge(Defaults, Opts).

%% @doc
%% Merges user provided worker options with defaults.
%% @param Opts: A map containing worker configuration.
%% @return A map with complete worker options.
new_worker_opts(Opts) when is_map(Opts) ->
    Defaults = #{
        prefix => ?DEFAULT_PREFIX,
        connection => #{},
        concurrency => 1,
        lockDuration => 30000, %% 30 seconds
        lockRenewTime => 15000 %% 15 seconds
    },
    maps:merge(Defaults, Opts).

%% @doc
%% Extracts or constructs Redis connection options for eredis.
%% This adapts the BullMQ 'connection' object to eredis style.
get_redis_opts(ConfigMap) ->
    Conn = maps:get(connection, ConfigMap, #{}),
    Host = maps:get(host, Conn, "127.0.0.1"),
    Port = maps:get(port, Conn, 6379),
    Database = maps:get(db, Conn, 0),
    Password = maps:get(password, Conn, ""),
    {Host, Port, Database, Password}.