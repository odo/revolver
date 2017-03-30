-module (revolver).

-behaviour (gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([balance/2, balance/3, map/2, start_link/3, pid/1, connect/1, transaction/2]).

-define(DEFAULTMINALIVERATIO,  1.0).
-define(DEFAULRECONNECTDELAY,  1000). % ms
-define(DEFAULTCONNECTATSTART, true).
-define(DEFAULTMAXMESSAGEQUEUELENGTH, undefined).

-type sup_ref()  :: {atom(), atom()}.

-record(state, {
    connected :: boolean(),
    supervisor :: sup_ref(),
    pid_table :: atom(),
    last_pid :: pid(),
    pids_count_original :: integer(),
    min_alive_ratio :: float(),
    reconnect_delay :: integer(),
    max_message_queue_length :: integer() | undefined
    }).

start_link(Supervisor, ServerName, Options) when is_map(Options) ->
    gen_server:start_link({local, ServerName}, ?MODULE, {Supervisor, Options}, []).

balance(Supervisor, BalancerName) ->
    revolver_sup:start_link(Supervisor, BalancerName, #{}).

balance(Supervisor, BalancerName, Options) ->
    revolver_sup:start_link(Supervisor, BalancerName, Options).

pid(PoolName) ->
    gen_server:call(PoolName, pid).

transaction(PoolName, Fun) ->
    case gen_server:call(PoolName, lease) of
      {ok, Pid} ->
        try
          Reply = Fun(Pid),
          ok = gen_server:call(PoolName, {release, Pid}),
          Reply
        catch
          Class:Reason ->
            ok = gen_server:call(PoolName, {release, Pid}),
            erlang:raise(Class, Reason, erlang:get_stacktrace())
          end;
      {error, Error} ->
        {error, Error}
    end.

map(ServerName, Fun) ->
    gen_server:call(ServerName, {map, Fun}).

connect(PoolName) ->
    gen_server:call(PoolName, connect).

init({Supervisor, Options}) ->
    MinAliveRatio         = maps:get(min_alive_ratio,  Options, ?DEFAULTMINALIVERATIO),
    ReconnectDelay        = maps:get(reconnect_delay,  Options, ?DEFAULRECONNECTDELAY),
    ConnectAtStart        = maps:get(connect_at_start, Options, ?DEFAULTCONNECTATSTART),
    MaxMessageQueueLength = maps:get(max_message_queue_length, Options, ?DEFAULTMAXMESSAGEQUEUELENGTH),

    PidTable = ets:new(pid_table, [private, duplicate_bag]),

    State = #state{
        connected                = false,
        supervisor               = Supervisor,
        pids_count_original      = undefined,
        min_alive_ratio          = MinAliveRatio,
        pid_table                = PidTable,
        last_pid                 = undefined,
        reconnect_delay          = ReconnectDelay,
        max_message_queue_length = MaxMessageQueueLength
    },
    maybe_connect(ConnectAtStart),
    {ok, State}.

maybe_connect(true) ->
  self() ! connect;
maybe_connect(_) ->
  noop.

% revolver is disconnected
handle_call(pid, _From, State = #state{connected = false}) ->
    {reply, {error, disconnected}, State};
% no limit on the message queue is defined
handle_call(pid, _From, State = #state{last_pid = LastPid, pid_table = PidTable, max_message_queue_length = undefined}) ->
    Pid = next_pid(PidTable, LastPid),
    {reply, Pid, State#state{last_pid = Pid}};
% message queue length is limited
handle_call(pid, _From, State = #state{last_pid = LastPid, pid_table = PidTable, max_message_queue_length = MaxMessageQueueLength}) when is_integer(MaxMessageQueueLength) ->
    {Pid, NextLastPid} = first_available(PidTable, LastPid, MaxMessageQueueLength),
    {reply, Pid, State#state{last_pid = NextLastPid}};

% revolver is disconnected
handle_call(lease, _From, State = #state{connected = false}) ->
    {reply, {error, disconnected}, State};
% no limit on the message queue is defined
handle_call(lease, _From, State = #state{last_pid = LastPid, pid_table = PidTable, max_message_queue_length = lease}) ->
  case first_available(PidTable, LastPid, lease) of
    {{error, Error}, NextLastPid} ->
      {reply, {error, Error}, State#state{last_pid = NextLastPid}};
    {{Pid, available}, _} ->
      true = ets:delete(PidTable, {Pid, available}),
      true = ets:insert(PidTable, {{Pid, leased}, undefined}),
      {reply, {ok, Pid}, State#state{last_pid = {Pid, leased}}}
  end;

handle_call({release, Pid}, _From, State = #state{pid_table = PidTable, last_pid = LastPid, max_message_queue_length = lease}) ->
  case ets:lookup(PidTable, {Pid, leased}) of
    [] -> noop;
    _ ->
      ets:delete(PidTable, {Pid, leased}),
      ets:insert(PidTable, {{Pid, available}, undefined})
  end,
  case LastPid of
    {Pid, leased} ->
      {reply, ok, State#state{last_pid = {Pid, available}}};
    _ ->
      {reply, ok, State}
  end;

handle_call({map, Fun}, _From, State = #state{pid_table = PidTable}) ->
    % we are reconnecting here to make sure we
    % have an up to date version of the pids
    StateNew = connect_internal(State),
    Pids     = ets:foldl(fun(Value, Acc) -> [element(1, Value)|Acc] end, [], PidTable),
    Reply    = lists:map(Fun, Pids),
    {reply, Reply, StateNew};

handle_call(connect, _From, State) ->
    NewSate = connect_internal(State),
    Reply =
    case NewSate#state.connected of
        true ->
            ok;
        false ->
            {error, not_connected}
    end,
    {reply, Reply, NewSate}.

handle_cast(_, State) ->
    throw("not implemented"),
    {noreply, State}.

handle_info(connect, State) ->
    {noreply, connect_internal(State)};

handle_info({pids, Pids}, State) ->
    {noreply, connect_internal(Pids, State)};

handle_info({'DOWN', _, _, Pid, _}, State = #state{supervisor = Supervisor, pid_table = PidTable, pids_count_original = PidsCountOriginal, min_alive_ratio = MinAliveRatio, max_message_queue_length = MaxMessageQueueLength}) ->
    error_logger:info_msg("~p: The process ~p (child of ~p) died.\n", [?MODULE, Pid, Supervisor]),
    delete_pid(PidTable, Pid, MaxMessageQueueLength),
    StateNew =
    case too_few_pids(PidTable, PidsCountOriginal, MinAliveRatio) of
        true ->
            error_logger:warning_msg("~p: Reloading children from supervisor ~p.\n", [?MODULE, Supervisor]),
            connect_async(State),
            State;
        false ->
            State
    end,
    {noreply, StateNew}.

terminate(_Reason, _State) -> ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

delete_pid(PidTable, Pid, lease) ->
    ets:match_delete(PidTable, {{Pid, '_'}, undefined});
delete_pid(PidTable, Pid, _) ->
    ets:delete(PidTable, Pid).


next_pid(PidTable, LastPid) ->
    case ets:next(PidTable, LastPid) of
        '$end_of_table' ->
            ets:first(PidTable);
        Value ->
            Value
    end.

first_available(PidTable, LastPid, MaxMessageQueueLength) ->
    first_available(PidTable, next_pid(PidTable, LastPid), LastPid, MaxMessageQueueLength).
% we arrived at the first pid (or we have only one in total)
% so we check one last time before we return overload
first_available(_PidTable, StartingPid, StartingPid, MaxMessageQueueLength) ->
    case available(StartingPid, MaxMessageQueueLength) of
        true ->
            {StartingPid, StartingPid};
        false  ->
            {{error, overload}, StartingPid}
    end;
% new pid candidate: check message queue
% and maybe recurse
first_available(PidTable, NextPid, StartingPid, MaxMessageQueueLength) ->
    case available(NextPid, MaxMessageQueueLength) of
        true ->
            {NextPid, NextPid};
        false  ->
            first_available(PidTable, next_pid(PidTable, NextPid), StartingPid, MaxMessageQueueLength)
    end.

available({_, Status}, lease) ->
    Status == available;

available(Pid, MaxMessageQueueLength) when is_integer(MaxMessageQueueLength) ->
    revolver_utils:message_queue_len(Pid) =< MaxMessageQueueLength.

too_few_pids(PidTable, PidsCountOriginal, MinAliveRatio) ->
    table_size(PidTable) / PidsCountOriginal < MinAliveRatio.

connect_async(#state{ supervisor = Supervisor}) ->
  self() ! {pids, revolver_utils:child_pids(Supervisor)}.

connect_internal(State = #state{ supervisor = Supervisor}) ->
    connect_internal(revolver_utils:child_pids(Supervisor), State).
connect_internal({error, supervisor_not_running}, State = #state{ pid_table = PidTable, reconnect_delay = ReconnectDelay}) ->
    ets:delete_all_objects(PidTable),
    schedule_reconnect(ReconnectDelay),
    State#state{ connected = false };
connect_internal(Pids, State = #state{ supervisor = Supervisor, pid_table = PidTable, reconnect_delay = ReconnectDelay, max_message_queue_length = MaxMessageQueueLength }) ->
    PidsNew         = lists:filter(fun(Pid) -> lookup(PidTable, Pid, MaxMessageQueueLength) =:= [] end, Pids),
    PidTableRecords = pid_table_records(PidsNew, MaxMessageQueueLength),
    true            = ets:insert(PidTable, PidTableRecords),
    StateNew        = State#state{ last_pid = ets:first(PidTable), pids_count_original = table_size(PidTable) },
    case table_size(PidTable) of
        0 ->
            error_logger:error_msg(
                "~p zero PIDs for ~p, disconnected.\n",
                [?MODULE, Supervisor]),
            schedule_reconnect(ReconnectDelay),
            StateNew#state{ connected = false };
        _ ->
            StateNew#state{ connected = true }
      end.

lookup(PidTable, Pid, lease) ->
  ets:lookup(PidTable, {Pid, available}) ++ ets:lookup(PidTable, {Pid, leased});
lookup(PidTable, Pid, _) ->
  ets:lookup(PidTable, Pid).

pid_table_records(Pids, lease) ->
    lists:map(fun(Pid) -> revolver_utils:monitor(Pid), {{Pid, available}, undefined} end, Pids);
pid_table_records(Pids, _) ->
    lists:map(fun(Pid) -> revolver_utils:monitor(Pid), {Pid, undefined} end, Pids).

schedule_reconnect(Delay) ->
    error_logger:error_msg("~p trying to reconnect in ~p ms.\n", [?MODULE, Delay]),
    erlang:send_after(Delay, self(), connect).


table_size(Table) ->
        {size, Count} = proplists:lookup(size, ets:info(Table)),
        Count.
