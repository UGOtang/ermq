%%%-------------------------------------------------------------------
%%% Minimal MessagePack encoder for Ermq.
%%% Implements necessary packing logic to communicate with Redis Lua scripts.
%%% Removes the need for external msgpack dependency.
%%%-------------------------------------------------------------------
-module(ermq_msgpack).

%% API
-export([pack/1]).

%% Encodes an Erlang term into MessagePack binary.
%% Supported types:
%% - integer
%% - float
%% - binary (string)
%% - atom (converted to binary, nil/null/undefined -> 0xC0)
%% - list (array)
%% - map
-spec pack(term()) -> binary().
pack(Term) ->
    iolist_to_binary(encode(Term)).

%% --- Internal Encoders ---

%% Null / Nil (0xC0)
encode(nil) -> <<16#C0>>;
encode(null) -> <<16#C0>>;
encode(undefined) -> <<16#C0>>;

%% Booleans (0xC2, 0xC3)
encode(false) -> <<16#C2>>;
encode(true) -> <<16#C3>>;

%% Integers
encode(I) when is_integer(I) ->
    if
        I >= 0, I =< 127 -> <<I>>; %% positive fixint
        I >= -32, I =< -1 -> <<(I band 16#FF)>>; %% negative fixint
        I >= 0, I =< 16#FF -> <<16#CC, I>>; %% uint8
        I >= 0, I =< 16#FFFF -> <<16#CD, I:16/big>>; %% uint16
        I >= 0, I =< 16#FFFFFFFF -> <<16#CE, I:32/big>>; %% uint32
        I >= 0 -> <<16#CF, I:64/big>>; %% uint64
        I >= -128 -> <<16#D0, (I band 16#FF)>>; %% int8

        I >= -32768 -> <<16#D1, I:16/big>>; %% int16
        I >= -2147483648 -> <<16#D2, I:32/big>>; %% int32
        true -> <<16#D3, I:64/big>> %% int64
    end;

%% Floats (always pack as float 64 - 0xCB)
encode(F) when is_float(F) ->
    <<16#CB, F:64/float-big>>;

%% Atoms (convert to binary string)
encode(A) when is_atom(A) ->
    encode(atom_to_binary(A, utf8));

%% Strings / Binaries
encode(B) when is_binary(B) ->
    Len = byte_size(B),
    if
        Len =< 31 -> <<(2#10100000 bor Len), B/binary>>; %% fixstr
        Len =< 16#FF -> <<16#D9, Len, B/binary>>; %% str8
        Len =< 16#FFFF -> <<16#DA, Len:16/big, B/binary>>; %% str16
        true -> <<16#DB, Len:32/big, B/binary>> %% str32
    end;

%% Lists (Arrays)
%% Note: Strings in Erlang are lists of integers, but for Redis interaction
%% in BullMQ, lists are usually Arrays of arguments.
%% We assume list is Array.
encode(L) when is_list(L) ->
    Len = length(L),
    Header = if
        Len =< 15 -> <<(2#10010000 bor Len)>>; %% fixarray
        Len =< 16#FFFF -> <<16#DC, Len:16/big>>; %% array16
        true -> <<16#DD, Len:32/big>> %% array32
    end,
    [Header | [encode(E) || E <- L]];

%% Maps
encode(M) when is_map(M) ->
    Len = maps:size(M),
    Header = if
        Len =< 15 -> <<(2#10000000 bor Len)>>; %% fixmap
        Len =< 16#FFFF -> <<16#DE, Len:16/big>>; %% map16
        true -> <<16#DF, Len:32/big>> %% map32
    end,
    %% Encode Key and Value pairs
    KV = maps:fold(fun(K, V, Acc) ->
        [Acc, encode(K), encode(V)]
    end, [], M),
    [Header | KV].