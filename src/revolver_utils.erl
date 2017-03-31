-module(revolver_utils).

-export([monitor/1, child_pids/1, message_queue_len/1, supervisor_name/1, alive/1]).

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

message_queue_len(Pid) ->
    case process_info(Pid, message_queue_len) of
        undefined ->
            0;
        {message_queue_len, N} ->
            N
    end.

alive(undefined) ->
    false;
alive(Process) when is_atom(Process) ->
    alive(erlang:whereis(Process));
alive(Process) when is_pid(Process) ->
    erlang:is_process_alive(Process).

supervisor_name(Name) ->
    postfix_atom(Name, "_revolver_sub").

postfix_atom(Name, Postfix) ->
    list_to_atom(atom_to_list(Name) ++ Postfix).
