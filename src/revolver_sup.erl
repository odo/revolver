-module(revolver_sup).
-behaviour(supervisor).

%% API
-export ([start_link/3, child_spec/3]).

%% Callbacks
-export ([init/1]).

-define(MIN_ALIVE_RATIO, 0.8).

child_spec(Supervisor, PoolName, Options) ->
    {
        revolver_utils:supervisor_name(PoolName),
        {revolver_sup, start_link, [Supervisor, PoolName, Options]},
        permanent, 1000, supervisor, [revolver_sup, revolver]
    }.

start_link(Supervisor, PoolName, Options) ->
    supervisor:start_link({local, revolver_utils:supervisor_name(PoolName)}, ?MODULE, {Supervisor, PoolName, Options}).

init({Supervisor, ServerName, Options}) ->
    Server = {
        ServerName,
        {revolver, start_link, [Supervisor, ServerName, Options]},
        permanent, 1000, worker, [revolver]
    },
    Children = [Server],
    RestartStrategy = {one_for_one, 10, 1},
    {ok, {RestartStrategy, Children}}.
