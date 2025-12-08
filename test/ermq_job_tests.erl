-module(ermq_job_tests).
-include_lib("eunit/include/eunit.hrl").

-define(TEST_PREFIX, <<"ermq_test">>).
-define(TEST_QUEUE, <<"test-queue">>).

%% Core Test Suite
job_flow_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(Client) ->
         [
             %% Long timeout for Redis IO and Script compilation
             {timeout, 15, fun() -> test_add_standard_job(Client) end},
             {timeout, 15, fun() -> test_job_retrieval(Client) end},
             {timeout, 15, fun() -> test_add_delayed_job(Client) end},
             {timeout, 15, fun() -> test_add_prioritized_job(Client) end},
             {timeout, 15, fun() -> test_update_progress(Client) end}
         ]
     end}.

%%%===================================================================
%%% Setup & Teardown
%%%===================================================================

setup() ->
    %% 1. Start dependencies
    application:start(crypto),
    %% 2. Load app info for priv_dir
    application:load(ermq),
    %% 3. Init script cache
    ermq_scripts:init(),
    %% 4. Connect Redis
    {ok, Client} = ermq_redis:start_link(#{}),
    %% 5. Clean dirty test data
    cleanup_test_data(Client),
    Client.

cleanup(Client) ->
    cleanup_test_data(Client),
    ermq_redis:stop(Client).

cleanup_test_data(Client) ->
    %% Warning: Do not run this on production!
    {ok, Keys} = ermq_redis:q(Client, ["KEYS", <<?TEST_PREFIX/binary, "*">>]),
    lists:foreach(fun(K) -> ermq_redis:q(Client, ["DEL", K]) end, Keys).

%%%===================================================================
%%% Test Cases
%%%===================================================================

%% Test adding a standard job
test_add_standard_job(Client) ->
    Data = #{<<"foo">> => <<"bar">>},
    Result = ermq_job:add(Client, ?TEST_PREFIX, ?TEST_QUEUE, <<"standard-job">>, Data),
    ?assertMatch({ok, _}, Result),
    {ok, JobId} = Result,
    ?assert(is_binary(JobId)),
    ?debugFmt("Standard Job Added: ~p", [JobId]).

%% Test adding and retrieving job data (JSON serialization)
test_job_retrieval(Client) ->
    Name = <<"retrieval-job">>,
    Data = #{
        <<"user">> => <<"alice">>, 
        <<"active">> => true, 
        <<"count">> => 100
    },
    Opts = #{jobId => <<"custom-id-123">>},
    
    %% Add Job
    {ok, JobId} = ermq_job:add(Client, ?TEST_PREFIX, ?TEST_QUEUE, Name, Data, Opts),
    ?assertEqual(<<"custom-id-123">>, JobId),

    %% Retrieve Job
    {ok, Job} = ermq_job:from_id(Client, ?TEST_PREFIX, JobId),
    
    %% Verify ID
    ?assertEqual(JobId, maps:get(id, Job)),
    
    %% Verify Data map
    RetrievedData = maps:get(<<"data">>, Job),
    ?assertEqual(Data, RetrievedData),
    
    %% Verify Default Opts
    RetrievedOpts = maps:get(<<"opts">>, Job),
    ?assertEqual(1, maps:get(<<"attempts">>, RetrievedOpts)). 

%% Test adding delayed job
test_add_delayed_job(Client) ->
    Data = #{<<"type">> => <<"delayed">>},
    Opts = #{delay => 5000}, %% 5 seconds
    Result = ermq_job:add(Client, ?TEST_PREFIX, ?TEST_QUEUE, <<"delayed-job">>, Data, Opts),
    
    ?assertMatch({ok, _}, Result),
    {ok, JobId} = Result,
    
    %% Verify existence in delayed ZSET (optional)
    DelayedKey = ermq_utils:to_key(?TEST_PREFIX, [?TEST_QUEUE, "delayed"]),
    {ok, Score} = ermq_redis:q(Client, ["ZSCORE", DelayedKey, JobId]),
    ?assertNotEqual(undefined, Score).

%% Test adding prioritized job
test_add_prioritized_job(Client) ->
    Data = #{<<"type">> => <<"priority">>},
    Opts = #{priority => 1}, %% High priority
    Result = ermq_job:add(Client, ?TEST_PREFIX, ?TEST_QUEUE, <<"prio-job">>, Data, Opts),
    ?assertMatch({ok, _}, Result).

%% Test updating progress
test_update_progress(Client) ->
    {ok, JobId} = ermq_job:add(Client, ?TEST_PREFIX, ?TEST_QUEUE, <<"progress-job">>, #{}),
    
    %% Update progress to 50
    ProgressResult = ermq_job:update_progress(Client, ?TEST_PREFIX, JobId, 50),
    ?assertMatch({ok, _}, ProgressResult),
    
    %% Verify via raw Redis HGET
    JobKey = ermq_utils:to_key(?TEST_PREFIX, JobId),
    {ok, ProgressVal} = ermq_redis:q(Client, ["HGET", JobKey, "progress"]),
    
    ?assertEqual(<<"50">>, ProgressVal).