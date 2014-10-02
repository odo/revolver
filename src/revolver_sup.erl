-module(revolver_sup).
-behaviour(supervisor).

%% API
-export ([start_link/4]).

%% Callbacks
-export ([init/1]).

-define(MIN_ALIVE_RATIO, 0.8).

start_link(Supervisor, ServerName, MinAliveRatio, ReconnectDelay) ->
    supervisor:start_link({local, supervisor_name(ServerName)}, ?MODULE, {Supervisor, ServerName, MinAliveRatio, ReconnectDelay}).

init({Supervisor, ServerName, MinAliveRatio, ReconnectDelay}) ->
    Server = {
        ServerName,
        {revolver, start_link, [Supervisor, ServerName, MinAliveRatio, ReconnectDelay]},
        permanent, 1000, worker, [revolver]
    },
    Children = [Server],
    RestartStrategy = {one_for_one, 10, 1},
    {ok, {RestartStrategy, Children}}.

supervisor_name(ServerName) ->
    list_to_atom(string:concat(atom_to_list(ServerName), "_sub")).
