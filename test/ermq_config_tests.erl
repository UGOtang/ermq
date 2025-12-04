-module(ermq_config_tests).
-include_lib("eunit/include/eunit.hrl").
-include("ermq.hrl").

%% @doc 测试队列配置的默认值合并
new_queue_opts_test() ->
    %% 情况1: 空配置
    Opts = ermq_config:new_queue_opts(#{}),
    ?assertEqual(?DEFAULT_PREFIX, maps:get(prefix, Opts)),
    ?assertEqual(#{}, maps:get(connection, Opts)),
    
    %% 情况2: 用户自定义配置
    UserOpts = #{prefix => <<"my-queue">>, connection => #{host => "redis"}},
    Merged = ermq_config:new_queue_opts(UserOpts),
    ?assertEqual(<<"my-queue">>, maps:get(prefix, Merged)),
    ?assertEqual("redis", maps:get(host, maps:get(connection, Merged))).

%% @doc 测试 Worker 配置的默认值合并
new_worker_opts_test() ->
    Opts = ermq_config:new_worker_opts(#{}),
    %% 验证 concurrency 默认值是否为 1
    ?assertEqual(1, maps:get(concurrency, Opts)),
    %% 验证锁时间
    ?assertEqual(30000, maps:get(lockDuration, Opts)).

%% @doc 测试从配置中提取 eredis 需要的参数
get_redis_opts_test() ->
    %% 默认情况
    {Host, Port, DB, _Pass} = ermq_config:get_redis_opts(#{}), %% 使用 _Pass 忽略
    ?assertEqual("127.0.0.1", Host),
    ?assertEqual(6379, Port),
    ?assertEqual(0, DB),
    
    %% 自定义情况
    Config = #{connection => #{host => "192.168.1.1", port => 9999, db => 2, password => "secret"}},
    {Host2, Port2, DB2, Pass2} = ermq_config:get_redis_opts(Config),
    ?assertEqual("192.168.1.1", Host2),
    ?assertEqual(9999, Port2),
    ?assertEqual(2, DB2),
    ?assertEqual("secret", Pass2).