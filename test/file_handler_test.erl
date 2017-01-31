-module(file_handler_test).

-define(SCAN_CHUNK_SIZE, 10).% 20b

-include_lib("eunit/include/eunit.hrl").

file_handler_test_() ->
  {Status, State} = file_handler:open("test/feed_file_handler_test.txt",
                                       #{chunk_size => ?SCAN_CHUNK_SIZE}),
  io:format(user, "Open Status: ~p~n", [Status]),
  case Status of
    ok ->
      {L1,L2,L3,L4} = read_lines(State),

      [?_assertMatch({ok, _, <<"HeelShoes">>}, L1),
       ?_assertMatch({ok, _, <<"HeelShoes">>}, L2),
       ?_assertMatch({ok, _, <<"HeelShoes">>}, L3),
       ?_assertMatch({eof, _, <<>>}, L4)
      ];

    _ ->
      io:format(user,"Open file falied: ~p, ~p~n", [Status, State]),
      [?_assertMatch(ok, Status)]
  end.


read_lines(State) ->
  L1 = {_,State1,_} = file_handler:read_line(State),
  L2 = {_,State2,_} = file_handler:read_line(State1),
  L3 = {_,State3,_} = file_handler:read_line(State2),
  L4 = {_,_,_} = file_handler:read_line(State3),
  {L1, L2, L3, L4}.
