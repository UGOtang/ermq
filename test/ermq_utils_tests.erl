-module(ermq_utils_tests).
-include_lib("eunit/include/eunit.hrl").

%% @doc 测试 UUID 生成格式
uuid_test() ->
    UUID = ermq_utils:v4(),
    %% 检查是否为二进制
    ?assert(is_binary(UUID)),
    %% UUID 标准长度为 36
    ?assertEqual(36, byte_size(UUID)),
    %% 简单检查中间是否有连字符
    ?assertEqual($-, binary:at(UUID, 8)),
    ?assertEqual($-, binary:at(UUID, 13)).

%% @doc 测试 Redis Key 拼接
to_key_test() ->
    Prefix = <<"bull">>,
    %% 测试单一部分拼接
    ?assertEqual(<<"bull:myqueue">>, ermq_utils:to_key(Prefix, "myqueue")),
    %% 测试多部分拼接
    ?assertEqual(<<"bull:myqueue:123">>, ermq_utils:to_key(Prefix, ["myqueue", 123])),
    %% 测试包含 binary 的拼接
    ?assertEqual(<<"bull:test:job">>, ermq_utils:to_key(Prefix, [<<"test">>, "job"])).

%% @doc 测试 JSON 编解码
json_test() ->
    Map = #{<<"key">> => <<"value">>, <<"num">> => 123},
    Encoded = ermq_utils:json_encode(Map),
    ?assert(is_binary(Encoded)),
    
    Decoded = ermq_utils:json_decode(Encoded),
    ?assertEqual(Map, Decoded).

%% @doc 测试空值检查
is_empty_test() ->
    ?assert(ermq_utils:is_empty(undefined)),
    ?assert(ermq_utils:is_empty(null)),
    ?assert(ermq_utils:is_empty([])),
    ?assert(ermq_utils:is_empty(<<>>)),
    ?assertNot(ermq_utils:is_empty(0)),
    ?assertNot(ermq_utils:is_empty(<<"a">>)).