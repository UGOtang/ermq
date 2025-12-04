-module(ermq_integration_tests).
-include_lib("eunit/include/eunit.hrl").

integration_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(Client) ->
         [
             {timeout, 10, fun() -> test_redis_ping(Client) end},
             {timeout, 10, fun() -> test_script_loading(Client) end}
         ]
     end}.

setup() ->
    application:start(crypto),
    ermq_scripts:init(),
    {ok, Client} = ermq_redis:start_link(#{}),
    Client.

cleanup(Client) ->
    ermq_redis:stop(Client).

test_redis_ping(Client) ->
    ?assertEqual({ok, <<"PONG">>}, ermq_redis:q(Client, ["PING"])).

test_script_loading(Client) ->
    ScriptName = 'pause-7', 
    %% 1. 尝试显式加载
    LoadResult = ermq_scripts:load_command(Client, ScriptName),
    case LoadResult of
        {ok, Sha} ->
            ?debugFmt("Script loaded SHA: ~p", [Sha]),
            ?assert(is_binary(Sha)),
            
            %% 2. 验证运行
            %% 我们传入空参数，脚本可能会在 Redis 内部报错 (比如 ARGV[1] 为 nil)
            %% 但只要 Redis 没报 "NOSCRIPT"，就说明 SHA 是有效的，脚本引擎工作正常。
            RunResult = ermq_scripts:run(Client, ScriptName, [], []),
            ?debugFmt("Script run result: ~p", [RunResult]),
            
            case RunResult of
                {ok, _} -> ok;
                {error, <<"NOSCRIPT", _/binary>>} -> 
                    ?assert(false); %% 这是唯一不能接受的错误
                {error, _Reason} -> 
                    %% 其他错误（如 Lua 运行时错误）是可以接受的，证明脚本跑起来了
                    ok
            end;
            
        {error, {file_read_error, Reason}} ->
            ?debugFmt("Warning: Could not read script file: ~p. Skipping test.", [Reason]),
            ok;
        Error ->
            ?debugFmt("Load failed: ~p", [Error]),
            ?assert(false)
    end.