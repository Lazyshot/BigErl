-module(slave).

-export([start/2, startSlave/2, waitForCommands/3]).
-import(erlang, [set_cookie/2]).
-import(config, [readConfig/1, getValue/2]).

%%-record(region, {region_id, node, table, start_id, stop_id}).
-record(get, {table, row, column, timestamp=0}).
-record(put, {table, row, mutations}).
%%-record(mutation, {column, value, timestamp=0}).

start(ConfFile, Cookie) -> 
	%Register slave with master
	register(slave, spawn(slave, startSlave, [ConfFile, Cookie])).

registerRegions(Master, []) ->
	Master;
registerRegions(Master, [_Region | _Regions]) ->
	%%_MRegion = #region{node=Master},
	Master.

splitRegion(Master, Region) ->
	RegionList = ets:tab2list(Region),
	{NewRegion1, NewRegion2} = lists:split(length(RegionList), RegionList),
	registerRegions(Master, [NewRegion1, NewRegion2]).

loadRegions(Config) ->
	DataPath = config:getValue(Config, data_path),
	case file:list_dir(DataPath) of
		{ok, Files} ->
			loadRegions(Files, []);
		{error, Reason} ->
			log:logError(Reason)
	end.

loadRegions([], Acc) ->
	Acc;

loadRegions([DataFile | Rest], Acc) ->
	Index = string:rstr(DataFile, ".dts"),
	if
		Index > 0 ->
			hashtable:loadTable(DataFile),
			NewAcc = [string:sub_string(DataFile, 1, Index - 1) | Acc];
		
		true ->
			NewAcc = Acc
	end,
	
	loadRegions(Rest, NewAcc).

%% Startup procedure
startSlave(ConfigFile, Cookie) ->
	
	Node = node(),
	
	set_cookie(Node, Cookie),
	
	Config = config:readConfig(ConfigFile),
	
	Master = config:getValue(Config, master_host),
	
	hashtable:loadTables(Config),
	
	{master, Master} ! {registerNode, Node},
	
	registerRegions(Master, loadRegions(Config)),
	
	waitForCommands(Config, Node, Master).



%% Receive commands, process, and act on them
waitForCommands(Config, Node, Master) ->
	
	receive
		
		{getRow, Region, Client, Session, Query} ->
			
			case hashtable:getRow(Region, Query#get.row) of
				error ->
					sendCommand(Master, {error, Client, Session, {get, Query}});
				Row ->
					sendCommand(Master, 
						{results, Node, Client, Session, Query, Row})
			end;
		
		{getCell, Region, Client, Session, Query} ->
			
			case hashtable:getCell(Region, Query#get.row, Query#get.column) of
				error ->
					sendCommand(Master, {error, Client, Session, {get, Query}});
				Cell ->
					sendCommand(Master, 
						{results, Node, Client, Session, Query, Cell})
			end;
		
		{put, Region, Client, Session, Put} ->
			
			case hashtable:mutateRow(Region, Put#put.row, Put#put.mutations) of
				true ->
					sendCommand(Master,
						{success, Client, Session, {put, Put}});
				false ->
					sendCommand(Master,
								{error, Client, Session, {put, Put}})			
			end; 
		
		Else ->
			
			log:logError(lists:append(["Unknown Command from ", Master , " :",  Else]))
	
	after 1000 ->
			
		sendCommand(Master, {heartbeat, Node})
	end,
	
	waitForCommands(Config, Node, Master).



%% Generic send command function
sendCommand(Master, Command) ->
	
	{master, Master} ! Command.
