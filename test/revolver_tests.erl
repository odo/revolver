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
        fun test_no_supervisor_init/0,
        fun test_no_children_init/0,
        fun test_no_children/0,
        fun test_no_supervisor/0,
        fun test_map/0,
        fun test_exit/0
      ]}
    ].

test_setup() ->
    application:start(sasl),
    meck:new(revolver_utils, [unstick, passthrough]),
    meck:expect(revolver_utils, monitor, fun(_) -> some_ref end).

test_teardown(_) ->
    meck:unload(revolver_utils).

default_options() ->
  #{ min_alive_ratio => 0.75, reconnect_delay => 1000}.

test_balance() ->
    meck:expect(revolver_utils, child_pids, fun(_) -> [1, 2, 3] end),
    {ok, StateInit}        = revolver:init({supervisor, default_options()}),
    {noreply, ReadyState}  = revolver:handle_info(connect, StateInit),
    {reply, Pid1, State1}  = revolver:handle_call(pid, me, ReadyState),
    ?assertEqual(3, Pid1),
    {reply, Pid2, State2}  = revolver:handle_call(pid, me, State1),
    ?assertEqual(1, Pid2),
    {reply, Pid3, State3}  = revolver:handle_call(pid, me, State2),
    ?assertEqual(2, Pid3),
    {reply, Pid4, _State4} = revolver:handle_call(pid, me, State3),
    ?assertEqual(3, Pid4).

test_balance_with_queue_limit_no_overload() ->
    Options = #{ max_message_queue_length => 10 },
    meck:expect(revolver_utils, child_pids, fun(_) -> [1, 2, 3] end),
    meck:expect(revolver_utils, message_queue_len, fun(_) -> 0 end),
    {ok, StateInit} = revolver:init({supervisor, Options}),
    {noreply, ReadyState}  = revolver:handle_info(connect, StateInit),
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
    {noreply, ReadyState}  = revolver:handle_info(connect, StateInit),
    {reply, Pid1, State1}  = revolver:handle_call(pid, me, ReadyState),
    ?assertEqual(1, Pid1),
    {reply, Pid2, _State2}  = revolver:handle_call(pid, me, State1),
    ?assertEqual(1, Pid2).


test_balance_with_queue_limit_one_overload() ->
    Options = #{ max_message_queue_length => 10 },
    meck:expect(revolver_utils, child_pids, fun(_) -> [1, 2, 3] end),
    meck:expect(revolver_utils, message_queue_len, fun(1) -> 100; (_) ->0 end),
    {ok, StateInit} = revolver:init({supervisor, Options}),
    {noreply, ReadyState}  = revolver:handle_info(connect, StateInit),
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
    {noreply, ReadyState}  = revolver:handle_info(connect, StateInit),
    {reply, Error1, State1}  = revolver:handle_call(pid, me, ReadyState),
    ?assertEqual({error, overload}, Error1),
    {reply, Error2, _State2}  = revolver:handle_call(pid, me, State1),
    ?assertEqual({error, overload}, Error2).


test_balance_with_lease() ->
    Options = #{ max_message_queue_length => lease, min_alive_ratio => 0.0},
    meck:expect(revolver_utils, child_pids, fun(_) -> [1, 2, 3] end),
    {ok, StateInit} = revolver:init({supervisor, Options}),
    {noreply, ReadyState}  = revolver:handle_info(connect, StateInit),
    {reply, {ok, 3}, LeaseState1}  = revolver:handle_call(lease, self(), ReadyState),
    {reply, {ok, 1}, LeaseState2}  = revolver:handle_call(lease, self(), LeaseState1),
    {reply, {ok, 2}, LeaseState3}  = revolver:handle_call(lease, self(), LeaseState2),
    {reply, {error,overload}, LeaseState4}  = revolver:handle_call(lease, self(), LeaseState3),
    {reply, ok, LeaseState5}  = revolver:handle_call({release, 1}, self(), LeaseState4),
    {reply, {ok, 1}, LeaseState6}  = revolver:handle_call(lease, self(), LeaseState5),
    {reply, {error,overload}, LeaseState7}  = revolver:handle_call(lease, self(), LeaseState6),
    {reply, ok, LeaseState8}  = revolver:handle_call({release, 3}, self(), LeaseState7),
    {reply, {ok, 3}, LeaseState9}  = revolver:handle_call(lease, self(), LeaseState8),
    {reply, ok, LeaseState10}  = revolver:handle_call({release, 1}, self(), LeaseState9),
    {reply, ok, LeaseState11}  = revolver:handle_call({release, 2}, self(), LeaseState10),
    {noreply, LeaseState12}   = revolver:handle_info({'DOWN', x, x, 2, x}, LeaseState11),
    {reply, ok, LeaseState13}  = revolver:handle_call({release, 2}, self(), LeaseState12),
    {reply, {ok, 1}, LeaseState14}  = revolver:handle_call(lease, self(), LeaseState13),
    {reply, {error,overload}, LeaseState15}  = revolver:handle_call(lease, self(), LeaseState14),
    ok.


test_map() ->
    meck:expect(revolver_utils, child_pids, fun(_) -> [1, 2, 3] end),
    {ok, StateInit} = revolver:init({supervisor, default_options()}),
    {reply, Reply, _} = revolver:handle_call({map, fun(Pid) -> Pid * 2 end}, x, StateInit),
    ?assertEqual([2, 4, 6], lists:sort(Reply)).

test_no_supervisor_init() ->
    {ok, StateInit}        = revolver:init({supervisor, default_options()}),
    {noreply, ReadyState}  = revolver:handle_info(connect, StateInit),
    {reply, Reply, _}      = revolver:handle_call(pid, me, ReadyState),
    ?assertEqual({error, disconnected}, Reply).

test_no_children_init() ->
    meck:expect(revolver_utils, child_pids, fun(_) -> [] end),
    {ok, StateInit}        = revolver:init({supervisor, default_options()}),
    {noreply, ReadyState}  = revolver:handle_info(connect, StateInit),
    {reply, Reply, _}      = revolver:handle_call(pid, me, ReadyState),
    ?assertEqual({error, disconnected}, Reply).

test_no_children() ->
    meck:expect(revolver_utils, child_pids, fun(_) -> [1, 2] end),
    {ok, StateInit}       = revolver:init({supervisor, default_options()}),
    {noreply, StateReady} = revolver:handle_info(connect, StateInit),
    {reply, 1, _}         = revolver:handle_call(pid, me, StateReady),
    meck:expect(revolver_utils, child_pids, fun(_) -> [] end),
    {noreply, StateDown1} = revolver:handle_info({'DOWN', x, x, 1, x}, StateReady),
    {reply, 2, _}         = revolver:handle_call(pid, me, StateDown1),
    {noreply, StateDown2} = revolver:handle_info({'DOWN', x, x, 2, x}, StateDown1),
    {reply, Reply, _}     = revolver:handle_call(pid, me, StateDown2),
    ?assertEqual({error, disconnected}, Reply).

test_no_supervisor() ->
    meck:expect(revolver_utils, child_pids, fun(_) -> [1, 2] end),
    {ok, StateInit}       = revolver:init({supervisor, default_options()}),
    {noreply, StateReady} = revolver:handle_info(connect, StateInit),
    meck:expect(revolver_utils, child_pids, fun(Arg) -> revolver_utils_meck_original:child_pids(Arg) end),
    {noreply, StateDown1}  = revolver:handle_info({'DOWN', x, x, 1, x}, StateReady),
    {noreply, StateDown2} = revolver:handle_info(connect, StateDown1),
    {reply, Reply, _}     = revolver:handle_call(pid, me, StateDown2),
    ?assertEqual({error, disconnected}, Reply).

test_exit() ->
    meck:expect(revolver_utils, child_pids, fun(_) -> [1, 2, 3] end),
    {ok, StateInit}       = revolver:init({supervisor, default_options()}),
    {noreply, StateReady} = revolver:handle_info(connect, StateInit),
    meck:expect(revolver_utils, child_pids, fun(_) -> [1, 3] end),
    {noreply, StateDown}   = revolver:handle_info({'DOWN', x, x, 2, x}, StateReady),
    {reply, Pid1, State1}  = revolver:handle_call(pid, x, StateDown),
    ?assertEqual(1, Pid1),
    {reply, Pid2, State2}  = revolver:handle_call(pid, x, State1),
    ?assertEqual(3, Pid2),
    {reply, Pid3, State3}  = revolver:handle_call(pid, x, State2),
    ?assertEqual(1, Pid3),
    meck:expect(revolver_utils, child_pids, fun(_) -> [1] end),
    {noreply, StateDown2}  = revolver:handle_info({'DOWN', x, x, 3, x}, State3),
    {reply, Pid4, State4}  = revolver:handle_call(pid, x, StateDown2),
    ?assertEqual(1, Pid4),
    {reply, Pid5, _State5} = revolver:handle_call(pid, x, State4),
    ?assertEqual(1, Pid5).

-endif.
