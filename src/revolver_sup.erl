-module(revolver_sup).
-behaviour(supervisor).

%% API
-export ([start_link/4, child_spec/4]).

%% Callbacks
-export ([init/1]).

-define(MIN_ALIVE_RATIO, 0.8).

child_spec(Supervisor, PoolName, MinAliveRatio, ReconnectDelay) ->
    {
        revolver_utils:supervisor_name(PoolName),
        {revolver_sup, start_link, [Supervisor, PoolName, MinAliveRatio, ReconnectDelay]},
        permanent, 1000, supervisor, [revolver_sup, revolver]
    }.

start_link(Supervisor, PoolName, MinAliveRatio, ReconnectDelay) ->
    supervisor:start_link({local, revolver_utils:supervisor_name(PoolName)}, ?MODULE, {Supervisor, revolver_utils:revolver_name(PoolName), MinAliveRatio, ReconnectDelay}).

init({Supervisor, ServerName, MinAliveRatio, ReconnectDelay}) ->
    Server = {
        ServerName,
        {revolver, start_link, [Supervisor, ServerName, MinAliveRatio, ReconnectDelay]},
        permanent, 1000, worker, [revolver]
    },
    Children = [Server],
    RestartStrategy = {one_for_one, 10, 1},
    {ok, {RestartStrategy, Children}}.
