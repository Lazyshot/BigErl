-module(hashtable).

-export([createTable/2, mutateRow/4, getCell/3, getRow/2, getTable/2, loadTables/1, loadTable/2, init/2, partitionRegion/2]).
-import(erlang, [localtime/0]).
-import(config, [readConfig/1, getValue/2]).

-record(cell, {column, value, timestamp}).
-record(mutation, {column, value, timestamp=0}).
-record(region, {name, table, data, start="", stop="", inmemory=0, file, fd=false}).


createTable(Name, Regions) -> 
	TRegion = #region{name=Name, table=Name, data=gb_trees:empty(),inmemory=0, file=lists:append(Name, ".big")},
	
	case file:open(TRegion#region.file, [read,write,binary]) of
		{ok, TFd} -> 
			Fd = TFd;
		{error, TReason} ->
			Fd = false,
			log:logError({error, TReason})
	end,
	
	NRegion = TRegion#region{fd=Fd},
	
	[NRegion | Regions].

%% Load tables may just be mapping a line number for every rowkey to increase throughput for writes/reads
%%
loadTables(_Config, [], Acc) ->
	Acc;
loadTables(Config, [Table | Rest], Acc) ->
	Index = string:str(Table, ".dts"),
	Data = fsync:loadRegion(Table, Index),
	Region = #region{name=Table,data=Data},
	loadTables(Config, Rest, lists:append([Acc, Region])).

loadTables(Config) -> 
	DataPath = config:getValue(Config, data_path),
	loadTables(Config, file:list_dir(DataPath), []).

loadTable(Tables, Table) ->
	createTable(Table, Tables).


getTable(Tables, Name) ->
	case lists:keyfind(Name, 2, Tables) of
		false ->
			error;
		Table ->
			Table
	end.

%% mutateRow(Tables, TableName, Row, Mutations)
%%		mutates a row with one or more changes
%%		this method may also require the recaching of the data and resyncing of the entire region
%%			- This may use more than necessary memory(~200 MB per region), which would lead to a low write throughput
%%			- Perhaps finding and replacing the row in the file manually for regions out of memory

mutateRow(Tables, TableName, RowKey, Mutations) ->
	Region = getTable(Tables, TableName),
	TableData = Region#region.data,
	
	case gb_trees:lookup(RowKey, TableData) of
		{value, Member} ->
			case Region#region.inmemory of
				1 ->
					RowData = mutateRow(Member, Mutations),
					NTable = Region#region{data=gb_trees:enter(RowKey, RowData, TableData)};
				0 ->
					{TRegion, Row} = fsync:readSingleLine(Region, Member),
					RowData = mutateRow(element(2, Row), Mutations),
					{NTable, success} = fsync:writeSingleLine(TRegion, Member, RowKey, RowData)
			end;
		none ->
			RowData = mutateRow([], Mutations),
			case Region#region.inmemory of
				1 ->
					NTable = Region#region{data=gb_trees:enter(RowKey, RowData, TableData)};
				0 ->
					{TTable, Pos} = fsync:appendSingleLine(Region, RowKey, RowData),
					NTable = TTable#region{data=gb_trees:enter(RowKey, Pos, TableData)}
			end
	end,
	
	lists:keystore(TableName, 2, Tables, NTable).


mutateRow(RowData, []) ->
	RowData;
mutateRow(RowData, [Mutation | RestMuts]) ->
	NRow = mutateRow(RowData, Mutation),
	mutateRow(NRow, RestMuts);

mutateRow(RowData, Mutation) ->
	Column = Mutation#mutation.column,
	Value = Mutation#mutation.value,
	CurrentTime = misc:getTimestamp(),
	
	NMember = lists:keydelete(Mutation#mutation.column, 2, RowData),
	lists:append(NMember, [#cell{column=Column, value=Value, timestamp=CurrentTime}]).


%% Split a region in 2 which is stored in memory
%% 	- Should I split a file in half and load each one? or load into memory then split and save?
partitionRegion(Tables, TableName) -> 
	Table = getTable(Tables, TableName),
	TableData = Table#region.data,
	
	case Table#region.inmemory of
		1 ->
			TableDList = gb_trees:to_list(TableData),
			
			%%Split the data of the regions in memory
			{Region1, Region2} = lists:split(trunc(length(TableDList) / 2), TableDList),
			RowKey1 = element(1, lists:nth(1, Region1)),
			RowKey2 = element(1, lists:nth(1, Region2)),
			
			Region1Stop = element(1, lists:last(Region1)),
			
			TmpList = lists:keydelete(TableName, 2, Tables),
			
			NRegion1 = #region{name=lists:append([TableName, "_",  md5:md5_hex(RowKey1)]), data=Region1, table=Table#region.table, start=Table#region.start, stop=Region1Stop, inmemory=1},
			NRegion2 = #region{name=lists:append([TableName, "_" , md5:md5_hex(RowKey2)]), data=Region2, table=Table#region.table, start=RowKey2, stop=Table#region.stop, inmemory=1},
			
			[NRegion1 | [NRegion2 | TmpList]];
		0 ->
			RegionFile = lists:append(TableName, ".big"),
			Region = fsync:loadRegion(RegionFile, TableName),
			TableDList = gb_trees:to_list(Region),
			
			%%Split the data of the regions in memory
			{Region1, Region2} = lists:split(trunc(length(TableDList) / 2), TableDList),
			
			RowKey1 = element(1, lists:nth(1, Region1)),
			RowKey2 = element(1, lists:nth(1, Region2)),
			
			Region1Stop = element(1, lists:last(Region1)),
			
			%%Deleting Old File/Data
			TmpList = lists:keydelete(TableName, 2, Tables),
			file:delete(RegionFile),
			
			Region1Name = lists:append([TableName, "_",  md5:md5_hex(RowKey1)]),
			Region2Name = lists:append([TableName, "_",  md5:md5_hex(RowKey2)]),

			Region1File = lists:append([Region1Name, ".big"]),
			Region2File = lists:append([Region2Name, ".big"]),
			
			fsync:saveRegion(Region1File, Region1),
			fsync:saveRegion(Region2File, Region2),
			Region1Disk = fsync:loadRegionDisk(Region1File, Region1Name),
			Region2Disk = fsync:loadRegionDisk(Region2File, Region2Name),
			NRegion1 = #region{name=Region1Name, data=Region1Disk, table=Table#region.table, start=Table#region.start, stop=Region1Stop},
			NRegion2 = #region{name=Region2Name, data=Region2Disk, table=Table#region.table, start=RowKey2, stop=Table#region.stop},
			
			[NRegion1 | [NRegion2 | TmpList]]
	
	end.

%% Obtain the value of a specific column, given the name of the column and respective row key
%% 
getCell(Table, RowKey, Column) ->
	case getRow(Table, RowKey) of
		
		error ->
			error;
		
		{Table, {RowKey, Member}} ->
			{RowKey, [findColumn(Member, Column)]}
	
	end.

%% Obtain the row data given a table/region and row key
%% Returns the entire member, which only holds the cells, not the key itself
%%		For regions out of memory this may require a disk seek to get the data
getRow(Table, RowKey) ->
	TableData = Table#region.data,
	
	case gb_trees:lookup(RowKey, TableData) of
		
		none ->
			error;
		
		{value, Member} ->
			case Table#region.inmemory of
				1 ->
					{RowKey, Member};
				0 ->
					fsync:readSingleLine(Table, Member)					
			end
	end.
			

%% Initialize the hashtable handling process
%%
init(Config, SlaveNode) ->
	Tables = loadTables(Config),
	startReceiving(Config, SlaveNode, Tables).


%% Receive commands from the slave process, which handles client connections
%% heartbeasts and region registration
startReceiving(Conf, SlaveNode, Tables) ->
	
	receive
		
		{getRow, Table, RowKey} ->
			{slave, SlaveNode} ! {results, getRow(Table, RowKey)},
			NewTables = Tables;
		
		{getCell, Table, RowKey, Column} -> 
			{slave, SlaveNode} ! {results, getCell(Table, RowKey, Column)},
			NewTables = Tables;
		
		{mutate, Table, RowKey, Mutations} ->
			NewTables = mutateRow(Tables, Table, RowKey, Mutations),
			{slave, SlaveNode} ! succeed
	
	end,
	
	startReceiving(Conf, SlaveNode, NewTables).

%% Find a specific column in a row which is used for getCell
findColumn(RowData, Column) ->
	case lists:keyfind(Column, 2, lists:reverse(lists:keysort(4, RowData))) of
		false ->
			error;
		Tuple ->
			Tuple
	end.

getRegionNameFromRowKey([Table | Rest], TableName, RowKey) ->
	TableStart = Table#region.start,
	TableStop = Table#region.stop,
	case (string:equal(Table#region.table, TableName) and ( (TableStart == "") or (TableStart =< RowKey)) and ((TableStop == "") or  (TableStop >= RowKey))) of
		true ->
			TableName;
		false ->
			getRegionNameFromRowKey(Rest, TableName, RowKey)
	end.
			