-module(revolver_roundrobin).

-export([init_state/1, new_pids/2, next_pid/1, pids/1, pid_down/2, delete_all_pids/1]).

-record(state, {
    pid_table :: atom(),
    last_pid :: pid(),
    max_message_queue_length :: integer() | undefined
    }).

% API for revolver
init_state(MaxMessageQueueLength) ->
  PidTable = ets:new(pid_table, [private, duplicate_bag]),
  #state{
      pid_table                = PidTable,
      last_pid                 = undefined,
      max_message_queue_length = MaxMessageQueueLength
  }.


% no limit on the message queue is defined
next_pid(State = #state{last_pid = LastPid, pid_table = PidTable, max_message_queue_length = undefined}) ->
  Pid = next_pid(PidTable, LastPid),
  {Pid, State#state{last_pid = Pid}};
% message queue length is limited
next_pid(State = #state{last_pid = LastPid, pid_table = PidTable, max_message_queue_length = MaxMessageQueueLength}) when is_integer(MaxMessageQueueLength) ->
  {Pid, NextLastPid} = first_available(PidTable, LastPid, MaxMessageQueueLength),
  {Pid, State#state{last_pid = NextLastPid}}.

pids(State = #state{pid_table = PidTable}) ->
  Pids = ets:foldl(fun(Value, Acc) -> [element(1, Value)|Acc] end, [], PidTable),
  {{ok, Pids}, State}.

delete_all_pids(State = #state{pid_table = PidTable}) ->
  ets:delete_all_objects(PidTable),
  {ok, State}.

new_pids(Pids, State = #state{pid_table = PidTable}) ->
  NewPids = lists:filter( fun(Pid) -> not ets:member(PidTable, Pid) end, Pids),
  ets:delete_all_objects(PidTable),
  PidTableRecords = pid_table_records(Pids),
  true            = ets:insert(PidTable, PidTableRecords),
  NextState       = State#state{ last_pid = ets:first(PidTable)},
  {ok, NewPids, NextState}.

pid_down(Pid, State = #state{pid_table = PidTable}) ->
  ets:delete(PidTable, Pid),
  {ok, State}.

% internal functions

next_pid(PidTable, LastPid) ->
    case catch ets:next(PidTable, LastPid) of
        '$end_of_table' ->
          ets:first(PidTable);
        {'EXIT', {badarg, _}} ->
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

available(Pid, MaxMessageQueueLength) when is_integer(MaxMessageQueueLength) ->
    revolver_utils:message_queue_len(Pid) =< MaxMessageQueueLength.

pid_table_records(Pids) ->
    lists:map(fun(Pid) -> {Pid, undefined} end, Pids).
