-module(revolver_tests).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

revolver_test_() ->
    [{foreach, local,
      fun test_setup/0,
      fun test_teardown/1,
      [
        fun test_balance/0,
        fun test_balance_with_queue_limit_no_overload/0,
        fun test_balance_with_queue_limit_no_overload_one_pid/0,
        fun test_balance_with_queue_limit_one_overload/0,
        fun test_balance_with_queue_limit_all_overload/0,
        fun test_balance_with_lease/0,
        fun test_reconnect_with_lease/0,
        fun test_no_supervisor_init/0,
        fun test_no_children_init/0,
        fun test_no_children/0,
        fun test_no_supervisor/0,
        fun test_map/0,
        fun test_exit/0,
        fun test_reload_every/0,
        fun test_transaction/0,
        fun test_transaction_with_error/0,
        fun test_transaction_with_error_with_kill_on_error/0
      ]}
    ].

test_setup() ->
    application:start(sasl),
    meck:new(revolver_utils, [unstick, passthrough]),
    meck:expect(revolver_utils, alive, fun(_) -> true end),
    meck:expect(revolver_utils, monitor, fun(_) -> noop end).

test_teardown(_) ->
    meck:unload(revolver_utils).

default_options() ->
  #{ min_alive_ratio => 0.75, reconnect_delay => 1000, connect_at_start => false}.

test_balance() ->
    meck:expect(revolver_utils, child_pids, fun(_) -> [1, 2, 3] end),
    {ok, StateInit}         = revolver:init({supervisor, default_options()}),
    {reply, ok, ReadyState} = revolver:handle_call(connect, me, StateInit),
    {reply, Pid1, State1}   = revolver:handle_call(pid, me, ReadyState),
    ?assertEqual(3, Pid1),
    {reply, Pid2, State2}   = revolver:handle_call(pid, me, State1),
    ?assertEqual(1, Pid2),
    {reply, Pid3, State3}   = revolver:handle_call(pid, me, State2),
    ?assertEqual(2, Pid3),
    {reply, Pid4, _State4}  = revolver:handle_call(pid, me, State3),
    ?assertEqual(3, Pid4).

test_balance_with_queue_limit_no_overload() ->
    Options = #{ max_message_queue_length => 10 },
    meck:expect(revolver_utils, child_pids, fun(_) -> [1, 2, 3] end),
    meck:expect(revolver_utils, message_queue_len, fun(_) -> 0 end),
    Self = self(),
    meck:expect(revolver_utils, monitor, fun(Pid) -> Self ! {monitor, Pid} end),
    {ok, StateInit} = revolver:init({supervisor, Options}),
    {reply, ok, ReadyState} = revolver:handle_call(connect, me, StateInit),
    ?assertEqual([3, 2, 1], monitors()),
    {reply, Pid1, State1}  = revolver:handle_call(pid, me, ReadyState),
    ?assertEqual(3, Pid1),
    {reply, Pid2, State2}  = revolver:handle_call(pid, me, State1),
    ?assertEqual(1, Pid2),
    {reply, Pid3, State3}  = revolver:handle_call(pid, me, State2),
    ?assertEqual(2, Pid3),
    {reply, Pid4, _State4} = revolver:handle_call(pid, me, State3),
    ?assertEqual(3, Pid4).

test_balance_with_queue_limit_no_overload_one_pid() ->
    Options = #{ max_message_queue_length => 10 },
    meck:expect(revolver_utils, child_pids, fun(_) -> [1] end),
    meck:expect(revolver_utils, message_queue_len, fun(_) -> 0 end),
    {ok, StateInit} = revolver:init({supervisor, Options}),
    {reply, ok, ReadyState} = revolver:handle_call(connect, me, StateInit),
    {reply, Pid1, State1}  = revolver:handle_call(pid, me, ReadyState),
    ?assertEqual(1, Pid1),
    {reply, Pid2, _State2}  = revolver:handle_call(pid, me, State1),
    ?assertEqual(1, Pid2).


test_balance_with_queue_limit_one_overload() ->
    Options = #{ max_message_queue_length => 10 },
    meck:expect(revolver_utils, child_pids, fun(_) -> [1, 2, 3] end),
    meck:expect(revolver_utils, message_queue_len, fun(1) -> 100; (_) ->0 end),
    {ok, StateInit} = revolver:init({supervisor, Options}),
    {reply, ok, ReadyState} = revolver:handle_call(connect, me, StateInit),
    {reply, Pid1, State1}  = revolver:handle_call(pid, me, ReadyState),
    ?assertEqual(3, Pid1),
    {reply, Pid2, State2}  = revolver:handle_call(pid, me, State1),
    ?assertEqual(2, Pid2),
    {reply, Pid3, _State3} = revolver:handle_call(pid, me, State2),
    ?assertEqual(3, Pid3).

test_balance_with_queue_limit_all_overload() ->
    Options = #{ max_message_queue_length => 10 },
    meck:expect(revolver_utils, child_pids, fun(_) -> [1, 2, 3] end),
    meck:expect(revolver_utils, message_queue_len, fun(_) -> 100 end),
    {ok, StateInit} = revolver:init({supervisor, Options}),
    {reply, ok, ReadyState} = revolver:handle_call(connect, me, StateInit),
    {reply, Error1, State1}  = revolver:handle_call(pid, me, ReadyState),
    ?assertEqual({error, overload}, Error1),
    {reply, Error2, _State2}  = revolver:handle_call(pid, me, State1),
    ?assertEqual({error, overload}, Error2).


test_balance_with_lease() ->
    Self = self(),
    meck:expect(revolver_utils, monitor, fun(Pid) -> Self ! {monitor, Pid} end),
    Options = #{ max_message_queue_length => lease, min_alive_ratio => 0.0},
    meck:expect(revolver_utils, child_pids, fun(_) -> [1, 2, 3] end),
    {ok, StateInit} = revolver:init({supervisor, Options}),
    {reply, ok, ReadyState} = revolver:handle_call(connect, me, StateInit),
    ?assertEqual([1, 2, 3], monitors()),
    {reply, Pid1, LeaseState1}  = revolver:handle_call(pid, self(), ReadyState),
    {reply, Pid2, LeaseState2}  = revolver:handle_call(pid, self(), LeaseState1),
    {reply, Pid3, LeaseState3}  = revolver:handle_call(pid, self(), LeaseState2),
    ?assertEqual([1, 2, 3], lists:sort([Pid1, Pid2, Pid3])),
    {reply, {error,overload}, LeaseState4}  = revolver:handle_call(pid, self(), LeaseState3),
    {reply, ok, LeaseState5}  = revolver:handle_call({release, 1}, self(), LeaseState4),
    {reply, 1, LeaseState6}  = revolver:handle_call(pid, self(), LeaseState5),
    {reply, {error,overload}, LeaseState7}  = revolver:handle_call(pid, self(), LeaseState6),
    {reply, ok, LeaseState8}  = revolver:handle_call({release, 3}, self(), LeaseState7),
    {reply, 3, LeaseState9}  = revolver:handle_call(pid, self(), LeaseState8),
    {reply, ok, LeaseState10}  = revolver:handle_call({release, 1}, self(), LeaseState9),
    {reply, ok, LeaseState11}  = revolver:handle_call({release, 2}, self(), LeaseState10),
    {noreply, LeaseState12}   = revolver:handle_info({'DOWN', x, x, 2, x}, LeaseState11),
    {reply, ok, LeaseState13}  = revolver:handle_call({release, 2}, self(), LeaseState12),
    {reply, 1, LeaseState14}  = revolver:handle_call(pid, self(), LeaseState13),
    {reply, {error,overload}, LeaseState15}  = revolver:handle_call(pid, self(), LeaseState14),
    {reply, ok, _} = revolver:handle_call(connect, me, LeaseState15),
    ?assertEqual([2], monitors()),
    ok.

  test_reconnect_with_lease() ->
      Options = #{ max_message_queue_length => lease, min_alive_ratio => 1.0},
      meck:expect(revolver_utils, child_pids, fun(_) -> [1, 2] end),
      {ok, StateInit} = revolver:init({supervisor, Options}),
      {reply, ok, ReadyState} = revolver:handle_call(connect, me, StateInit),
      {reply, 2, LeaseState1}  = revolver:handle_call(pid, self(), ReadyState),
      {reply, ok, ReconnectState} = revolver:handle_call(connect, me, LeaseState1),
      {reply, 1, LeaseState2}  = revolver:handle_call(pid, self(), ReconnectState),
      {reply, {error, overload}, _}  = revolver:handle_call(pid, self(), LeaseState2),
      ok.


test_map() ->
    meck:expect(revolver_utils, child_pids, fun(_) -> [1, 2, 3] end),
    {ok, StateInit} = revolver:init({supervisor, default_options()}),
    {reply, Reply, _} = revolver:handle_call({map, fun(Pid) -> Pid * 2 end}, x, StateInit),
    ?assertEqual([2, 4, 6], lists:sort(Reply)).

test_no_supervisor_init() ->
    {ok, StateInit}        = revolver:init({supervisor, default_options()}),
    {reply, {error, not_connected}, ReadyState} = revolver:handle_call(connect, me, StateInit),
    {reply, Reply, _}      = revolver:handle_call(pid, me, ReadyState),
    ?assertEqual({error, disconnected}, Reply).

test_no_children_init() ->
    meck:expect(revolver_utils, child_pids, fun(_) -> [] end),
    {ok, StateInit}        = revolver:init({supervisor, default_options()}),
    {reply, {error, not_connected}, ReadyState} = revolver:handle_call(connect, me, StateInit),
    {reply, Reply, _}      = revolver:handle_call(pid, me, ReadyState),
    ?assertEqual({error, disconnected}, Reply).

test_no_children() ->
    meck:expect(revolver_utils, child_pids, fun(_) -> [1, 2] end),
    {ok, StateInit}       = revolver:init({supervisor, default_options()}),
    {reply, ok, StateReady} = revolver:handle_call(connect, me, StateInit),
    {reply, 1, _}         = revolver:handle_call(pid, me, StateReady),
    meck:expect(revolver_utils, child_pids, fun(_) -> [] end),
    {noreply, StateDown1} = revolver:handle_info({'DOWN', x, x, 1, x}, StateReady),
    StateDown2 = receive_and_handle(StateDown1),
    {reply, {error, disconnected}, _} = revolver:handle_call(pid, me, StateDown2).

test_reload_every() ->
    meck:expect(revolver_utils, child_pids, fun(_) -> [1, 2] end),
    {ok, StateInit} = revolver:init({supervisor, #{ min_alive_ratio => 1.0, reconnect_delay => 1000, connect_at_start => false, reload_every => 2}}),
    {reply, ok, StateReady} = revolver:handle_call(connect, me, StateInit),
    {reply, 1, _}         = revolver:handle_call(pid, me, StateReady),
    meck:expect(revolver_utils, child_pids, fun(_) -> [] end),
    {noreply, StateDown1} = revolver:handle_info({'DOWN', x, x, 1, x}, StateReady),
    {error, timeout} = receive_and_handle(StateDown1),
    {noreply, StateDown2} = revolver:handle_info({'DOWN', x, x, 2, x}, StateDown1),
    ?assert({error, timeout} =/= receive_and_handle(StateDown2)).

test_no_supervisor() ->
    meck:expect(revolver_utils, child_pids, fun(_) -> [1, 2] end),
    {ok, StateInit}       = revolver:init({supervisor, default_options()}),
    {reply, ok, StateReady} = revolver:handle_call(connect, me, StateInit),
    meck:expect(revolver_utils, child_pids, fun(Arg) -> revolver_utils_meck_original:child_pids(Arg) end),
    {noreply, StateDown1}  = revolver:handle_info({'DOWN', x, x, 1, x}, StateReady),
    {noreply, StateDown2} = revolver:handle_info(connect, StateDown1),
    StateReconnect = receive_and_handle(StateDown2),
    {reply, Reply, _}     = revolver:handle_call(pid, me, StateReconnect),
    ?assertEqual({error, disconnected}, Reply).

test_exit() ->
    Self = self(),
    meck:expect(revolver_utils, monitor, fun(Pid) -> Self ! {monitor, Pid} end),
    meck:expect(revolver_utils, child_pids, fun(_) -> [1, 2, 3] end),
    {ok, StateInit}       = revolver:init({supervisor, default_options()}),
    {reply, ok, StateReady} = revolver:handle_call(connect, me, StateInit),
    ?assertEqual([3, 2, 1], monitors()),
    meck:expect(revolver_utils, child_pids, fun(_) -> [1, 3] end),
    {noreply, StateDown}   = revolver:handle_info({'DOWN', x, x, 2, x}, StateReady),
    StateReconnect = receive_and_handle(StateDown),
    {reply, Pid1, State1}  = revolver:handle_call(pid, x, StateReconnect),
    ?assertEqual(1, Pid1),
    {reply, Pid2, State2}  = revolver:handle_call(pid, x, State1),
    ?assertEqual(3, Pid2),
    {reply, Pid3, State3}  = revolver:handle_call(pid, x, State2),
    ?assertEqual(1, Pid3),
    meck:expect(revolver_utils, child_pids, fun(_) -> [1] end),
    {noreply, StateDown2}  = revolver:handle_info({'DOWN', x, x, 3, x}, State3),
    StateReconnect2 = receive_and_handle(StateDown2),
    {reply, Pid4, State4}  = revolver:handle_call(pid, x, StateReconnect2),
    ?assertEqual(1, Pid4),
    {reply, Pid5, State5} = revolver:handle_call(pid, x, State4),
    ?assertEqual(1, Pid5),
    meck:expect(revolver_utils, child_pids, fun(_) -> [1, 2, 3] end),
    {reply, ok, _} = revolver:handle_call(connect, me, State5),
    ?assertEqual([3, 2], monitors()).

test_transaction() ->
    TestProcess = self(),
    DummyPid = spawn(fun() -> timer:sleep(100) end),
    meck:expect(revolver, pid, fun(pool_name) -> DummyPid end),
    meck:expect(revolver, release, fun(pool_name, Pid) -> TestProcess ! {released, Pid}, ok end),
    Result = revolver:transaction(pool_name, fun(_) -> transaction end),
    ?assertEqual(Result, transaction),
    assert_receive({released, DummyPid}),
    alive(DummyPid).

test_transaction_with_error() ->
    TestProcess = self(),
    DummyPid = spawn(fun() -> timer:sleep(100) end),
    meck:expect(revolver, pid, fun(pool_name) -> DummyPid end),
    meck:expect(revolver, release, fun(pool_name, Pid) -> TestProcess ! {released, Pid}, ok end),
    Reply =
    try
      revolver:transaction(pool_name, fun(_) -> throw(tantrum) end),
      false
    catch
      throw:tantrum ->
        true
    end,
    ?assert(Reply),
    assert_receive({released, DummyPid}),
    ?assert(alive(DummyPid)).

test_transaction_with_error_with_kill_on_error() ->
    TestProcess = self(),
    DummyPid = spawn(fun() -> timer:sleep(100) end),
    meck:expect(revolver, pid, fun(pool_name) -> DummyPid end),
    meck:expect(revolver, release, fun(pool_name, Pid) -> TestProcess ! {released, Pid}, ok end),
    Reply =
    try
      revolver:transaction(pool_name, fun(_) -> throw(tantrum) end, true),
      false
    catch
      throw:tantrum ->
        true
    end,
    ?assert(Reply),
    ?assertEqual(false, alive(DummyPid)).

assert_receive(Message) ->
    receive
      Message -> ?assert(true)
    after 100 ->
      ?assertEqual(true, timeout)
end.

receive_and_handle(State) ->
    receive
      Message ->
        {noreply, NewState} = revolver:handle_info(Message, State),
        NewState
      after 10 ->
        {error, timeout}
    end.

monitors() ->
  monitors([]).
monitors(Pids) ->
  receive
    {monitor, Pid} ->
        monitors([Pid | Pids])
  after
    10 -> Pids
end.

alive(Pid) ->
  process_info(Pid) =/= undefined.

-endif.
