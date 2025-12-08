%%%-------------------------------------------------------------------
%%% Utility functions corresponding to src/utils.ts in BullMQ.
%%% Includes UUID generation, Redis key formatting, and JSON helpers.
%%%-------------------------------------------------------------------
-module(ermq_utils).

%% API
-export([v4/0]).
-export([to_key/2]).
-export([json_encode/1, json_decode/1]).
-export([is_empty/1]).
-export([to_binary/1]).


%%%===================================================================
%%% API Functions
%%%===================================================================

%% Generates a UUID v4 string.
v4() ->
    <<U0:32, U1:16, _:4, U2:12, _:2, U3:62>> = crypto:strong_rand_bytes(16),
    <<UUID:128>> = <<U0:32, U1:16, 4:4, U2:12, 2:2, U3:62>>,
    list_to_binary(uuid_to_string(UUID)).

%% Constructs a Redis key with the configured prefix.
to_key(Prefix, Parts) when is_list(Parts) ->
    RealParts = case is_flat_string(Parts) of
        true -> [Parts];
        false -> Parts
    end,
    Joined = join(RealParts, <<":">>),
    <<Prefix/binary, ":", Joined/binary>>;
to_key(Prefix, Part) ->
    to_key(Prefix, [Part]).

%% Encodes an Erlang map/term to JSON binary.
json_encode(Term) ->
    jsone:encode(Term).

%% Decodes a JSON binary to an Erlang map.
json_decode(Binary) ->
    jsone:decode(Binary, [{object_format, map}]).

%% Checks if a value is "empty".
is_empty(undefined) -> true;
is_empty(null) -> true;
is_empty(<<>>) -> true;
is_empty([]) -> true;
is_empty(_) -> false.

%% Converts various types to binary for Redis compatibility.
to_binary(V) when is_binary(V) -> V;
to_binary(V) when is_list(V) -> list_to_binary(V);
to_binary(V) when is_integer(V) -> integer_to_binary(V);
to_binary(V) when is_float(V) -> float_to_binary(V, [{decimals, 10}, compact]);
to_binary(V) when is_atom(V) -> atom_to_binary(V, utf8).

%%%===================================================================
%%% Internal Functions
%%%===================================================================

uuid_to_string(I) ->
    io_lib:format("~8.16.0b-~4.16.0b-~4.16.0b-~4.16.0b-~12.16.0b", 
        [I bsr 96, (I bsr 80) band 16#ffff, (I bsr 64) band 16#ffff, 
         (I bsr 48) band 16#ffff, I band 16#ffffffffffff]).

join([], _Sep) -> <<>>;
join([Head], _Sep) -> to_binary(Head);
join([Head | Tail], Sep) ->
    H = to_binary(Head),
    T = join(Tail, Sep),
    <<H/binary, Sep/binary, T/binary>>.

is_flat_string([]) -> false;
is_flat_string([H|_]) when is_integer(H) -> true;
is_flat_string(_) -> false.