-module (revolver).

-behaviour (gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([balance/2, balance/3, map/2, start_link/3, pid/1, pid_down/2, release/2, connect/1, transaction/2, transaction/3]).

-define(DEFAULTMINALIVERATIO,  1.0).
-define(DEFAULRECONNECTDELAY,  1000). % ms
-define(DEFAULTCONNECTATSTART, true).
-define(DEFAULTMAXMESSAGEQUEUELENGTH, undefined).
-define(DEFAULTRELOADEVERY, 1).

% stacktrace

-ifdef(OTP_RELEASE). %% this implies 21 or higher
-define(EXCEPTION(Class, Reason, Stacktrace), Class:Reason:Stacktrace).
-define(GET_STACK(Stacktrace), Stacktrace).
-else.
-define(EXCEPTION(Class, Reason, _), Class:Reason).
-define(GET_STACK(_), erlang:get_stacktrace()).
-endif.

-type sup_ref()  :: {atom(), atom()}.

-record(state, {
    backend :: atom(),
    backend_state :: any,
    connected :: boolean(),
    supervisor :: sup_ref(),
    pids_count_original :: integer(),
    min_alive_ratio :: float(),
    reload_every :: integer(),
    down_count :: integer(),
    reconnect_delay :: integer()
    }).

start_link(Supervisor, ServerName, Options) when is_map(Options) ->
    gen_server:start_link({local, ServerName}, ?MODULE, {Supervisor, Options}, []).

balance(Supervisor, BalancerName) ->
    revolver_sup:start_link(Supervisor, BalancerName, #{}).

balance(Supervisor, BalancerName, Options) ->
    revolver_sup:start_link(Supervisor, BalancerName, Options).

pid(PoolName) ->
    gen_server:call(PoolName, pid).

pid_down(PoolName, Pid) ->
  gen_server:call(PoolName, {pid_down, Pid}).

release(PoolName, Pid) ->
  gen_server:cast(PoolName, {release, Pid}).

transaction(PoolName, Fun) ->
  transaction(PoolName, Fun, false).

transaction(PoolName, Fun, KillOnError) ->
    case ?MODULE:pid(PoolName) of
      Pid when is_pid(Pid) ->
        try
          Reply = Fun(Pid),
          ok = ?MODULE:release(PoolName, Pid),
          Reply
        catch
          ?EXCEPTION(Class, Reason, Stacktrace) ->
            case KillOnError of
              false ->
                ok = ?MODULE:release(PoolName, Pid);
              true ->
                dead = kill(Pid)
            end,
            erlang:raise(Class, Reason, ?GET_STACK(Stacktrace))
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
    ReloadEvery           = maps:get(reload_every,     Options, ?DEFAULTRELOADEVERY),
    ReconnectDelay        = maps:get(reconnect_delay,  Options, ?DEFAULRECONNECTDELAY),
    ConnectAtStart        = maps:get(connect_at_start, Options, ?DEFAULTCONNECTATSTART),
    MaxMessageQueueLength = maps:get(max_message_queue_length, Options, ?DEFAULTMAXMESSAGEQUEUELENGTH),

    Backend = case MaxMessageQueueLength of
      lease -> revolver_lease;
      _     -> revolver_roundrobin
    end,

    BackendState = apply(Backend, init_state, [MaxMessageQueueLength]),

    State = #state{
        backend                  = Backend,
        backend_state            = BackendState,
        connected                = false,
        supervisor               = Supervisor,
        pids_count_original      = undefined,
        min_alive_ratio          = MinAliveRatio,
        reload_every             = ReloadEvery,
        down_count               = 0,
        reconnect_delay          = ReconnectDelay
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
handle_call(pid, _From, State = #state{ backend = Backend, backend_state = BackendState}) ->
    {Pid, NextBackendState} = apply(Backend, next_pid, [BackendState]),
    NextState = State#state{backend_state = NextBackendState},
    % under high load, the pid might have died in the meantime
    case is_pid(Pid) andalso not revolver_utils:alive(Pid) of
      true ->
        {ok, NextBackendState2} = apply(Backend, pid_down, [Pid, NextBackendState]),
        NextState2 = NextState#state{backend_state = NextBackendState2},
        handle_call(pid, internal, NextState2);
      false ->
        {reply, Pid, NextState}
    end;

handle_call({pid_down, Pid}, _From, State = #state{ backend = Backend, backend_state = BackendState}) ->
  {Reply, NextBackendState} = apply(Backend, pid_down, [Pid, BackendState]),
  NextState = State#state{backend_state = NextBackendState},
  {reply, Reply, NextState};

handle_call({release, Pid}, _From, State = #state{ backend = Backend, backend_state = BackendState}) ->
  {Reply, NextBackendState} =
  case revolver_utils:alive(Pid) of
    true ->
      apply(Backend, release,  [Pid, BackendState]);
    false ->
      apply(Backend, pid_down, [Pid, BackendState])
  end,
  NextState = State#state{backend_state = NextBackendState},
  {reply, Reply, NextState};


handle_call({map, Fun}, _From, State = #state{ backend = Backend, backend_state = BackendState }) ->
    ConnectedSate = connect_internal(State),
    {{ok, Pids}, NextBackendState} = apply(Backend, pids, [BackendState]),
    Reply     = lists:map(Fun, Pids),
    NextState = ConnectedSate#state{backend_state = NextBackendState},
    {reply, Reply, NextState};

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

handle_cast({release, Pid}, State) ->
  {reply, _, NextState} = handle_call({release, Pid}, internal, State),
  {noreply, NextState};

handle_cast(_, State) ->
    throw("not implemented"),
    {noreply, State}.

handle_info(connect, State) ->
    {noreply, connect_internal(State)};

handle_info({pids, Pids}, State) ->
    {noreply, connect_internal(Pids, State)};

handle_info({'DOWN', _, _, Pid, Reason}, State = #state{supervisor = Supervisor, pids_count_original = PidsCountOriginal, backend = Backend, backend_state = BackendState, min_alive_ratio = MinAliveRatio, down_count = DownCount , reload_every = ReloadEvery}) ->
    error_logger:info_msg("~p: The process ~p (child of ~p) died for reason ~p.\n", [?MODULE, Pid, Supervisor, Reason]),
    {ok, NextBackendState} = apply(Backend, pid_down, [Pid, BackendState]),
    {{ok, Pids}, NextBackendState2} = apply(Backend, pids, [NextBackendState]),
    Connected = Pids =/= [],
    NewDownCount = DownCount + 1,
    case ((NewDownCount rem ReloadEvery) == 0) andalso  too_few_pids(Pids, PidsCountOriginal, MinAliveRatio) of
        true ->
            error_logger:warning_msg("~p: Reloading children from supervisor ~p (connected: ~p).\n", [?MODULE, Supervisor, Connected]),
            connect_async(State);
        false ->
            noop
    end,
    {noreply, State#state{ connected = Connected, backend_state = NextBackendState2, down_count = NewDownCount }}.

terminate(_Reason, _State) -> ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

too_few_pids(Pids, PidsCountOriginal, MinAliveRatio) ->
    length(Pids) / PidsCountOriginal < MinAliveRatio.

connect_async(#state{ supervisor = Supervisor}) ->
  Self = self(),
  spawn_link(fun() -> Self ! {pids, revolver_utils:child_pids(Supervisor)} end).

connect_internal(State = #state{ supervisor = Supervisor}) ->
    connect_internal(revolver_utils:child_pids(Supervisor), State).
connect_internal({error, supervisor_not_running}, State = #state{ reconnect_delay = ReconnectDelay, backend = Backend, backend_state = BackendState}) ->
    {ok, NextBackendState} = apply(Backend, delete_all_pids, [BackendState]),
    schedule_reconnect(ReconnectDelay),
    State#state{ connected = false, backend_state = NextBackendState };
connect_internal(Pids, State = #state{ supervisor = Supervisor, reconnect_delay = ReconnectDelay, backend = Backend, backend_state = BackendState }) ->
    {ok, NewPids, NextBackendState} = apply(Backend, new_pids, [Pids, BackendState]),
    [revolver_utils:monitor(NewPid) || NewPid <- NewPids],
    case length(Pids) of
        0 ->
            error_logger:error_msg(
                "~p zero PIDs for ~p, disconnected.\n",
                [?MODULE, Supervisor]),
            schedule_reconnect(ReconnectDelay),
            State#state{ connected = false, backend_state = NextBackendState };
        PidCount ->
            State#state{ connected = true,  backend_state = NextBackendState,  pids_count_original = PidCount}
      end.

schedule_reconnect(Delay) ->
    error_logger:error_msg("~p trying to reconnect in ~p ms.\n", [?MODULE, Delay]),
    erlang:send_after(Delay, self(), connect).

kill(Pid) ->
  kill(Pid, normal).

kill(Pid, Reason) ->
  case process_info(Pid) == undefined of
    true ->
      dead;
    false ->
      exit(Pid, Reason),
      timer:sleep(10),
      kill(Pid, kill)
  end.
