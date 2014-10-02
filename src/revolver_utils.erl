-module(revolver_utils).

-export([monitor/1, child_pids/1]).

monitor(Pid) ->
    erlang:monitor(process, Pid).

child_pids(Supervisor) ->
    case alive(Supervisor) of
        false ->
            error_logger:error_msg("~p: Supervisor ~p not running, disconnected.\n", [?MODULE, Supervisor]),
            {error, supervisor_not_running};
        _ ->
            [ Pid || {_, Pid, _, _} <- supervisor:which_children(Supervisor), is_pid(Pid)]
    end.

alive(undefined) ->
    false;
alive(Supervisor) when is_atom(Supervisor) ->
    alive(erlang:whereis(Supervisor));
alive(Supervisor) when is_pid(Supervisor) ->
    erlang:is_process_alive(Supervisor).


