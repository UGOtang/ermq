%%%-------------------------------------------------------------------
%%% @doc
%%% Utility functions corresponding to src/utils.ts in BullMQ.
%%% Includes UUID generation, Redis key formatting, and JSON helpers.
%%% @end
%%%-------------------------------------------------------------------
-module(ermq_utils).

%% API
-export([v4/0]).
-export([to_key/2]).
-export([json_encode/1, json_decode/1]).
-export([is_empty/1]).

-include("ermq.hrl").

%%%===================================================================
%%% API Functions
%%%===================================================================

%% @doc
%% Generates a UUID v4 string.
v4() ->
    <<U0:32, U1:16, _:4, U2:12, _:2, U3:62>> = crypto:strong_rand_bytes(16),
    <<UUID:128>> = <<U0:32, U1:16, 4:4, U2:12, 2:2, U3:62>>,
    list_to_binary(uuid_to_string(UUID)).

%% @doc
%% Constructs a Redis key with the configured prefix.
%% Handles ambiguity between "String" (list of ints) and "List of Parts".
to_key(Prefix, Parts) when is_list(Parts) ->
    %% Determine if Parts is a string ("queue") or a list of parts (["queue", "id"])
    RealParts = case is_flat_string(Parts) of
        true -> [Parts];
        false -> Parts
    end,
    Joined = join(RealParts, <<":">>),
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
%% Checks if a value is "empty".
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

%% Standard join implementation
join([], _Sep) -> <<>>;
join([Head], _Sep) -> to_binary(Head);
join([Head | Tail], Sep) ->
    H = to_binary(Head),
    T = join(Tail, Sep),
    <<H/binary, Sep/binary, T/binary>>.

to_binary(V) when is_binary(V) -> V;
to_binary(V) when is_list(V) -> list_to_binary(V);
to_binary(V) when is_integer(V) -> integer_to_binary(V);
to_binary(V) when is_atom(V) -> atom_to_binary(V, utf8).

%% Helper to distinguish string vs list of parts
is_flat_string([]) -> false; %% Empty list treated as empty parts list
is_flat_string([H|_]) when is_integer(H) -> true; %% "abc" -> [97, 98, 99]
is_flat_string(_) -> false.