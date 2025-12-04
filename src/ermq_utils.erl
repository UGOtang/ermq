%%%-------------------------------------------------------------------
%%% @doc
%%% Utility functions corresponding to src/utils.ts
%%% Includes UUID generation, Redis key formatting, and JSON helpers.
%%% @end
%%%-------------------------------------------------------------------
-module(ermq_utils).

%% API
-export([v4/0]).
-export([to_key/2]).
-export([json_encode/1, json_decode/1]).
-export([is_empty/1]).

-include("include/ermq.hrl").

%%%===================================================================
%%% API Functions
%%%===================================================================

%% @doc
%% Generates a UUID v4 string.
%% @return A binary string representing a UUID.
v4() ->
    <<U0:32, U1:16, _:4, U2:12, _:2, U3:62>> = crypto:strong_rand_bytes(16),
    %% Set the version to 4 (binary 0100) and variant to 2 (binary 10)
    <<UUID:128>> = <<U0:32, U1:16, 4:4, U2:12, 2:2, U3:62>>,
    list_to_binary(uuid_to_string(UUID)).

%% @doc
%% Constructs a Redis key with the configured prefix.
%% Format: prefix:queue_name:part
%% @param Prefix: The global prefix.
%% @param Parts: A generic identifier or list of parts to append.
to_key(Prefix, Parts) when is_list(Parts) ->
    Joined = join(Parts, <<":">>),
    <<Prefix/binary, ":", Joined/binary>>;
to_key(Prefix, Part) ->
    to_key(Prefix, [Part]).

%% @doc
%% Encodes an Erlang map/term to JSON binary.
json_encode(Term) ->
    jsone:encode(Term).

%% @doc
%% Decodes a JSON binary to an Erlang map.
json_decode(Binary) ->
    jsone:decode(Binary, [{object_format, map}]).

%% @doc
%% Checks if a value is "empty" (undefined, null, or empty string/list).
is_empty(undefined) -> true;
is_empty(null) -> true;
is_empty(<<>>) -> true;
is_empty([]) -> true;
is_empty(_) -> false.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

uuid_to_string(I) ->
    io_lib:format("~8.16.0b-~4.16.0b-~4.16.0b-~4.16.0b-~12.16.0b", 
        [I bsr 96, (I bsr 80) band 16#ffff, (I bsr 64) band 16#ffff, 
         (I bsr 48) band 16#ffff, I band 16#ffffffffffff]).

join([Head | Tail], Sep) ->
    TailJoined = join(Tail, Sep),
    SafeHead = to_binary(Head),
    <<SafeHead/binary, Sep/binary, TailJoined/binary>>;
join([Head], _Sep) ->
    to_binary(Head);
join([], _Sep) ->
    <<>>.

to_binary(V) when is_binary(V) -> V;
to_binary(V) when is_list(V) -> list_to_binary(V);
to_binary(V) when is_integer(V) -> integer_to_binary(V);
to_binary(V) when is_atom(V) -> atom_to_binary(V, utf8).