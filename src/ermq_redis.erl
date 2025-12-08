%%%-------------------------------------------------------------------
%%% Redis connection wrapper.
%%% Corresponds to src/classes/redis-connection.ts in BullMQ.
%%% Handles the creation and management of Redis connections using eredis.
%%%-------------------------------------------------------------------
-module(ermq_redis).

%% API
-export([start_link/1, stop/1]).
-export([command/2, pipeline/2]).
-export([q/2, qp/2]). %% Aliases for convenience

%%%===================================================================
%%% API Functions
%%%===================================================================

%% Starts a new Redis connection process.
%% Opts: A map containing connection configuration or proplist.
start_link(Opts) ->
    %% Extract eredis options using our config helper
    {Host, Port, Database, Password} = ermq_config:get_redis_opts(Opts),
    
    EredisOpts = [
        {host, Host},
        {port, Port},
        {database, Database},
        {password, Password},
        {reconnect_sleep, 100}
    ],
    
    eredis:start_link(EredisOpts).

%% Closes the Redis connection.
stop(Client) when is_pid(Client) ->
    eredis:stop(Client).

%% Executes a single Redis command.
command(Client, Command) ->
    eredis:q(Client, Command).

%% Alias for command/2.
q(Client, Command) ->
    command(Client, Command).

%% Executes a pipeline of Redis commands.
pipeline(Client, Pipeline) ->
    eredis:qp(Client, Pipeline).

%% Alias for pipeline/2.
qp(Client, Pipeline) ->
    pipeline(Client, Pipeline).