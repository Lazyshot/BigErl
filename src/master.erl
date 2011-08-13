-module(master).


-export([start/1, startServer/1, startReceiving/1]).
-import(erlang, [set_cookie/2]).
-record(node, {node_info, heartbeat}).
-record(qry, {table, row, column="", timestamp=0}).
-record(region, {region_id, node, table, start_id, stop_id}).
-record(put, {table, row, mutations}).
%%-record(mutation, {column, value, timestamp=0}).


start(Cookie) ->
	register(master, spawn(master, startServer, [Cookie])).

%Saving and Loading Root and Meta tables
%	Root is necessary for definition of tables
%	Meta not so much, because that defines where data lies, which
%	is reported by each slave on startup to the master. No need for saving/loading.
%
loadRoot(Config) ->
	Datapath = config:getValue(Config, data_path),
	
	case filelib:is_regular(lists:append([Datapath, "/root.dts"])) of
		true ->
			ets:file2tab(lists:append([Datapath, "/root.dts"]));
		_Else -> 
			ets:new('.ROOT.', [named_table, {keypos, 2}])
	end.

saveRoot(Config) ->
	Datapath = config:getValue(Config, data_path),
	ets:tab2file('.ROOT.', lists:append([Datapath, "/root.dts"])).

%% saveMeta(Config) ->
%% 	Datapath = config:getValue(Config, data_path),
%% 	ets:tab2file('_META_', lists:append([Datapath, "/meta.dts"])).

loadMeta(Config) ->
	Datapath = config:getValue(Config, data_path),
	
	case filelib:is_regular(lists:append([Datapath, "/meta.dts"])) of
		true ->
			ets:file2tab(lists:append([Datapath, "/meta.dts"]));
		_Else ->
			ets:new('_META_', [named_table, {keypos, 2}])
	end.
	
	
%%Startup procedure
startServer(Cookie) ->
	MasterNode = node(),
	set_cookie(MasterNode, Cookie),
	
	Configuration = config:readConfig("master.conf"),
	
	loadRoot(Configuration),
	loadMeta(Configuration),
	
	log:logInfo("Server started and listening for slave registration"),
	startReceiving(Configuration).


%%Start receiving node registration, client commands, and other
%% necessary communication from slaves and clients
startReceiving(Config) ->
	log:logInfo("Iterate Receive"),
	
	receive
		%Receive from API Call - Client
		%{Table, RowKey, [Column]}
		{get, Client, Session, Query} ->
			case findSlaveByRowKey(Query#qry.table, Query#qry.row) of
				false ->
					{client, Client} ! {error, "RowKey doesn't exist"};
				{Node, Region} ->
					if
						Query#qry.column == "" ->
							{slave, Node} ! {getCell, Region, Client, Session, Query };
						true ->
							{slave, Node} ! {getRow, Region, Client, Session, Query }
					end
			end;
		
		%%mutate Rows
		{put, Client, Session, Put} ->
			case findSlaveByRowKey(Put#put.table, Put#put.row) of
				false ->
					{client, Client} ! {error, "RowKey doesn't exist"};
				{Node, Region} ->
						{slave, Node} ! {put, Region, Client, Session, Put}
			end;
		
		%%Registering Nodes
		{registerNode, Node} ->
			
			NewNode = #node{node_info=Node,heartbeat=misc:getTimestamp()},
			ets:insert('.ROOT.', NewNode),
			
			log:logInfo(lists:append(["New Slave Registered: ", atom_to_list(Node)])),
			log:logDebug(lists:append(["All Slaves: ", ets:tab2list('.ROOT.')]));
		
		%%Register Regions
		{registerRegion, Node, Region, Table, Start, Stop} ->
			ets:insert('_META_', #region{region_id=Region,node=Node,table=Table,start_id=Start,stop_id=Stop}),
			log:logDebug(lists:append(["New Region Registered from ", atom_to_list(Node), ": ", Region, " for ", Table, " Ranges ", Start, "-", Stop]));

		%%Health of nodes
		{heartbeat, Node} ->
			
			log:logDebug(lists:append(["Heartbeat from ", atom_to_list(Node)])),
			
			case ets:lookup('.ROOT.', Node) of
				[NodeObj] ->
					
					NewNode = NodeObj#node{heartbeat=misc:getTimestamp()},
					ets:insert('.ROOT.', NewNode);
				[] ->
					
					log:logError(lists:append(["Node not registered, but receiving heartbeat: ", atom_to_list(Node)]))
			end;
		%%Slave Died (Error)
		{killed, Node, Error} ->
			
			log:logInfo(lists:append(["Node disconnected (", atom_to_list(Node), "): ", Error])),
			ets:delete('.ROOT.', Node);
		
		%%Results from a get call
		{results, Node, Client, Session, Query, Results} ->
			log:logInfo(io_lib:format("Results from node ~p with query ~p: ~p ~n", [Node, Query, Results])),
			{client, Client} ! {results, Session, Results}
	
		%Cmd ->
		%	log:logError(lists:append(["Unknown Command: ", Cmd]))
	
	end,
	
	%%Saving Root and Meta Tables - Potentially unnecessary (Maybe everytime a change of table properties)
	saveRoot(Config),
	%%saveMeta(Config),
	
	%%No matter the reason, keep receiving commands
	startReceiving(Config).

%%Function name is selfexplanatory - Finds the proper {Slave, RegionId} for a certain Table, RowKey pair
findSlaveByRowKey(_Table, _RowKey, []) ->
	false;

findSlaveByRowKey(Table, RowKey, [Region | Rest]) ->
	%table, start_id, stop_id
	if
		Region#region.table == Table ->
			StartId = Region#region.start_id,
			StopId = Region#region.stop_id,
			
			if 
				RowKey >= StartId andalso RowKey =< StopId ->
					{Region#region.node, Region#region.region_id};
				true ->
					findSlaveByRowKey(Table, RowKey, Rest)
			end;
		true ->
			findSlaveByRowKey(Table, RowKey, Rest)
	end.
	
findSlaveByRowKey(Table, RowKey) ->
	findSlaveByRowKey(Table, RowKey, ets:tab2list(Table)).

