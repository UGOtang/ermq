%%%-------------------------------------------------------------------
%%% Job definition and manipulation module.
%%% Corresponds to src/classes/job.ts.
%%% Handles Job creation, adding to Redis via Lua scripts, and retrieval.
%%% Uses MessagePack for Lua script arguments.
%%%-------------------------------------------------------------------
-module(ermq_job).

%% API
-export([add/5, add/6]).
-export([from_id/3]).
-export([update_progress/4]).

-include("ermq.hrl").

%%%===================================================================
%%% API Functions
%%%===================================================================

%% Adds a new job to the queue.
%% Args: Client, Prefix, QueueName, Name, Data, Opts
add(Client, Prefix, QueueName, Name, Data, Opts) ->
    JobOpts = maps:merge(?DEFAULT_JOB_OPTS, Opts),
    
    %% Generate ID if missing
    JobId = case maps:get(jobId, JobOpts, undefined) of
        undefined -> ermq_utils:v4();
        CustomId -> ermq_utils:to_binary(CustomId)
    end,
    
    Timestamp = maps:get(timestamp, JobOpts),
    Delay = maps:get(delay, JobOpts, 0),
    Priority = maps:get(priority, JobOpts, undefined),
    
    %% Prepare Data and Opts
    JsonData = ermq_utils:json_encode(Data),
    %% Note: Scripts expect Opts to be msgpacked, not JSON.
    PackedOpts = pack_args(JobOpts),
    
    {ScriptName, Keys, Args} = prepare_add_script(
        Prefix, QueueName, JobId, Name, Timestamp, Delay, Priority, PackedOpts, JsonData
    ),
    
    case ermq_scripts:run(Client, ScriptName, Keys, Args) of
        {ok, _Result} -> {ok, JobId};
        Error -> Error
    end.

add(Client, Prefix, QueueName, Name, Data) ->
    add(Client, Prefix, QueueName, Name, Data, #{}).

%% Retrieves a Job from Redis by ID.
from_id(Client, Prefix, JobId) ->
    JobKey = ermq_utils:to_key(Prefix, [JobId]),
    case ermq_redis:q(Client, ["HGETALL", JobKey]) of
        {ok, []} -> {error, not_found};
        {ok, ListData} ->
            MapData = list_to_map(ListData),
            RawData = maps:get(<<"data">>, MapData, <<>>),
            RawOpts = maps:get(<<"opts">>, MapData, <<>>),
            RawReturn = maps:get(<<"returnvalue">>, MapData, <<>>),
            
            %% Decode JSON fields
            Job = MapData#{
                <<"data">> => safe_json_decode(RawData),
                <<"opts">> => safe_json_decode(RawOpts),
                <<"returnvalue">> => safe_json_decode(RawReturn),
                id => JobId
            },
            {ok, Job};
        Error -> Error
    end.

%% Updates job progress.
update_progress(Client, Prefix, JobId, Progress) ->
    JsonProgress = ermq_utils:json_encode(Progress),
    Keys = [ermq_utils:to_key(Prefix, JobId), ermq_utils:to_key(Prefix, "events")],
    Args = [ermq_utils:to_binary(JobId), JsonProgress],
    ermq_scripts:run(Client, 'updateProgress-3', Keys, Args).

%%%===================================================================
%%% Internal Functions
%%%===================================================================

prepare_add_script(Prefix, _QueueName, JobId, Name, Timestamp, Delay, Priority, PackedOpts, JsonData) ->
    %% Redis Keys (Standard 9 keys for BullMQ v4/v5)
    WaitKey = ermq_utils:to_key(Prefix, <<"wait">>),
    PausedKey = ermq_utils:to_key(Prefix, <<"paused">>),
    MetaKey = ermq_utils:to_key(Prefix, <<"meta">>),
    IdKey = ermq_utils:to_key(Prefix, <<"id">>),
    CompletedKey = ermq_utils:to_key(Prefix, <<"completed">>),
    DelayedKey = ermq_utils:to_key(Prefix, <<"delayed">>),
    ActiveKey = ermq_utils:to_key(Prefix, <<"active">>),
    EventsKey = ermq_utils:to_key(Prefix, <<"events">>),
    MarkerKey = ermq_utils:to_key(Prefix, <<"marker">>),
    
    BaseKeys = [
        WaitKey, PausedKey, MetaKey, IdKey, 
        CompletedKey, DelayedKey, ActiveKey, EventsKey, MarkerKey
    ],
    
    %% MsgPack Arguments Construction
    %% Structure based on 'destructure.lua':
    %% [prefix, customId, name, timestamp, parentKey, parentDepKey, parent, repeatJobKey, deduplicationKey]
    ArgList = [
        Prefix,
        JobId,
        Name,
        Timestamp,
        nil, %% parentKey
        nil, %% parentDependenciesKey
        nil, %% parent
        nil, %% repeatJobKey
        nil  %% deduplicationKey
    ],
    
    PackedArgs = pack_args(ArgList),
    
    %% Final Args to Redis: [PackedArgs, JsonData, PackedOpts]
    FinalArgs = [PackedArgs, JsonData, PackedOpts],

    if
        Delay > 0 ->
             {'addDelayedJob-6', BaseKeys, FinalArgs ++ [ermq_utils:to_binary(Delay)]};

        Priority =/= undefined ->
             {'addPrioritizedJob-9', BaseKeys, FinalArgs ++ [ermq_utils:to_binary(Priority)]};

        true ->
            {'addStandardJob-9', BaseKeys, FinalArgs}
    end.

%% Helper wrapper for msgpack:pack using apply/3.
%% This helps bypass strict static type checking (eqwalize) regarding 'nil' atoms
%% and ensures proper return types.
-spec pack_args(term()) -> binary().
pack_args(Term) ->
    case apply(msgpack, pack, [Term]) of
        Bin when is_binary(Bin) -> Bin;
        {error, Reason} -> error({msgpack_pack_error, Reason});
        Other -> error({msgpack_unexpected_return, Other})
    end.

list_to_map(List) -> list_to_map(List, #{}).
list_to_map([], Acc) -> Acc;
list_to_map([K, V | T], Acc) -> list_to_map(T, maps:put(K, V, Acc)).

safe_json_decode(<<>>) -> #{};
safe_json_decode(null) -> #{};
safe_json_decode(Bin) ->
    try ermq_utils:json_decode(Bin)
    catch _:_ -> Bin
    end.