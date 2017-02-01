-module(file_handler).
%%% This modul helps dealing with large files
%%% open returns a file handler to be given in any external method
%%% it enables to read chunks of data,
%%% or lines - by searching for the first line feed (\n)

%DEFINES
-define(SCAN_CHUNK_SIZE, 1024 * 1000 * 10).% 10MB
-define(HEAD_LENGTH, 3000).% 10MB
-define(DEFAULT_MAP,#{chunk_size => ?SCAN_CHUNK_SIZE}).
-define(NEWLINE, <<"\n">>).

%RECORDS
-record(state, {filename,
                file_descriptor,
                chunk_size = ?SCAN_CHUNK_SIZE,
                tail = <<>> }).


%EXPORTS
-export([open/1]).
-export([open/2]).
-export([read_chunk/1]).
-export([read_line/1]).

-spec open(string()) -> {ok, #state{}} | {error, any()}.
open(FilePath) -> open(FilePath, ?DEFAULT_MAP).

-spec open(string(), #{ chunk_size := integer()}) ->
  {ok, #state{}} | {error, any()}.
open(FilePath, #{chunk_size := ChunkSize})
  when is_integer(ChunkSize), ChunkSize > 0 ->
  case file:open(FilePath, [binary]) of
    {ok, FileDescriptor} ->
      {ok, #state{filename = FilePath,
                  file_descriptor = FileDescriptor,
                  chunk_size = ChunkSize
                 }};
    Else -> Else
  end.

read_chunk(#state{ file_descriptor = undefined} ) -> {eof, <<>>};
read_chunk(#state{ file_descriptor = Source,
                   chunk_size = ChunkSize } = State) ->
  case file:read(Source, ChunkSize) of
    {ok, Chunk} -> {ok, State, Chunk};
    eof -> State1 = close(State), {eof, State1, <<>>}
  end.

read_line(#state{file_descriptor = undefined} = State) -> {eof, State};
read_line(#state{tail = Tail} = State) ->
  case binary:split(Tail, ?NEWLINE) of
    [Line, Rest] ->
      {ok, State#state{tail = Rest}, Line};
    [_] ->
      case read_chunk(State) of
        {ok, State1, Chunk} ->
          read_line(State1#state{ tail = <<Tail/binary, Chunk/binary >> });
        {eof, State1, _} -> {eof, State1, <<>>}
      end
  end.

close(#state{file_descriptor = undefined} = State) -> State;
close(#state{file_descriptor = FileDescriptor} = State) ->
  file:close(FileDescriptor),
  State#state{file_descriptor = undefined}.
