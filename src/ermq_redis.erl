%%%-------------------------------------------------------------------
%%% @doc
%%% Redis connection wrapper.
%%% Corresponds to src/classes/redis-connection.ts in BullMQ.
%%% Handles the creation and management of Redis connections using eredis.
%%% @end
%%%-------------------------------------------------------------------
-module(ermq_redis).

%% API
-export([start_link/1, stop/1]).
-export([command/2, pipeline/2]).
-export([q/2, qp/2]). %% Aliases for convenience

-include("include/ermq.hrl").

%%%===================================================================
%%% API Functions
%%%===================================================================

%% @doc
%% Starts a new Redis connection process.
%% @param Opts: A map containing connection configuration (host, port, etc.)
%%              or a simplified proplist.
%% @return {ok, Pid} or {error, Reason}.
start_link(Opts) ->
    %% Extract eredis options using our config helper
    {Host, Port, Database, Password} = ermq_config:get_redis_opts(Opts),
    
    %% Prepare eredis options
    %% We can add more options like reconnection policy here if needed.
    EredisOpts = [
        {host, Host},
        {port, Port},
        {database, Database},
        {password, Password},
        {reconnect_sleep, 100} %% Retry connection every 100ms if failed
    ],
    
    %% Start the eredis client
    eredis:start_link(EredisOpts).

%% @doc
%% Closes the Redis connection.
%% @param Client: The Pid of the redis connection.
stop(Client) when is_pid(Client) ->
    eredis:stop(Client).

%% @doc
%% Executes a single Redis command.
%% @param Client: The Pid of the redis connection.
%% @param Command: A list representing the command parts (e.g., ["GET", "key"]).
%% @return {ok, Result} | {error, Reason}.
command(Client, Command) ->
    eredis:q(Client, Command).

%% @doc
%% Alias for command/2.
q(Client, Command) ->
    command(Client, Command).

%% @doc
%% Executes a pipeline of Redis commands.
%% @param Client: The Pid of the redis connection.
%% @param Pipeline: A list of commands (e.g., [["SET", "k", "v"], ["GET", "k"]]).
%% @return {ok, Results} | {error, Reason}.
pipeline(Client, Pipeline) ->
    eredis:qp(Client, Pipeline).

%% @doc
%% Alias for pipeline/2.
qp(Client, Pipeline) ->
    pipeline(Client, Pipeline).