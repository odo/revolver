-module(revolver_lease).

-export([init_state/1, new_pids/2, next_pid/1, release/2, pids/1, pid_down/2, delete_all_pids/1]).

-record(state, {
    pids_available :: list(),
    pids_leased    :: any()
  }).

% API for revolver
init_state(lease) ->
  #state{
      pids_available = [],
      pids_leased    = sets:new()
  }.

delete_all_pids(State) ->
  {ok, State#state{pids_available = [], pids_leased = sets:mew()}}.

new_pids(Pids, State = #state{ pids_leased = PidsLeased, pids_available = PidsAvailable}) ->
  PidsSet = sets:from_list(Pids),

  NewPids =sets:to_list(sets:subtract(PidsSet, sets:union(PidsLeased, sets:from_list(PidsAvailable)))),

  NextPidsLeased    = sets:intersection(PidsSet, PidsLeased),
  NextPidsAvailable = sets:to_list(sets:subtract(PidsSet, NextPidsLeased)),
  NextState         = State#state{ pids_available = NextPidsAvailable, pids_leased = NextPidsLeased },
  {ok, NewPids, NextState}.

next_pid(State = #state{pids_available = []}) ->
  {{error, overload}, State};
next_pid(State = #state{pids_available = [NextPid | RemainingPids], pids_leased = PidsLeased}) ->
  NextState = State#state{ pids_available = RemainingPids, pids_leased = sets:add_element(NextPid, PidsLeased) },
  {NextPid, NextState}.

release(Pid, State = #state{pids_available = PidsAvailable, pids_leased = PidsLeased}) ->
  case sets:is_element(Pid, PidsLeased) of
    true ->
      NextState = State#state{pids_available = PidsAvailable ++ [Pid], pids_leased = sets:del_element(Pid, PidsLeased)},
      {ok, NextState};
    false ->
      {ok, State}
  end.

pid_down(Pid, State = #state{pids_available = PidsAvailable, pids_leased = PidsLeased}) ->
  NextState =
  case sets:is_element(Pid, PidsLeased) of
    true ->
      State#state{pids_leased = sets:del_element(Pid, PidsLeased)};
    false ->
      State#state{pids_available = lists:delete(Pid, PidsAvailable)}
  end,
  {ok, NextState}.

pids(State = #state{pids_available = PidsAvailable, pids_leased = PidsLeased}) ->
  Pids = lists:flatten([PidsAvailable, sets:to_list(PidsLeased)]),
  {{ok, Pids}, State}.
