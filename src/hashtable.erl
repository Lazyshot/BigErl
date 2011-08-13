-module(hashtable).

-export([createTable/1, getTable/1, getTables/0, mutateRow/3, getCell/3, getRow/2, loadTables/1, loadTable/1]).
-import(erlang, [localtime/0]).
-import(config, [readConfig/1, getValue/2]).

-record(cell, {column, value, timestamp}).
-record(row, {rowkey, cells}).
-record(mutation, {column, value, timestamp=0}).


createTable(Name) -> 
	%%ets:new(Name, [public, named_table, ordered_set]).
	dets:open_file(Name).

getTable( Name) ->
	dets:table(Name).

getTables() ->
	dets:all().


loadTables(Config, [Table | Rest]) ->
	Index = string:str(Table, ".dts"),
	
	if
		 Index > 0 ->
			dets:file2tab(Table)
	end,
	
	loadTables(Config, Rest).

loadTables(Config) -> 
	DataPath = config:getValue(Config, data_path),
	loadTables(Config, file:list_dir(DataPath)).

loadTable(TableFile) ->
	dets:file2tab(TableFile).
	

mutateRow(_Table, _RowKey, []) ->
	true;
mutateRow(Table, RowKey, [Mutation | RestMuts]) ->
	mutateRow(Table, RowKey, Mutation),
	mutateRow(Table, RowKey, RestMuts);
mutateRow(Table, RowKey, Mutation) ->
	Column = Mutation#mutation.column,
	Value = Mutation#mutation.value,
	CurrentTime = misc:getTimestamp(),
	case dets:lookup(Table, RowKey) of
		[Member] ->
			ExistMember = Member#row{cells= lists:append(Member#row.cells, [#cell{column=Column, value=Value, timestamp=CurrentTime}])};
		[] ->
			ExistMember = #row{rowkey=RowKey, cells=[#cell{column=Column,value=Value, timestamp=CurrentTime}]}
	end,
	dets:insert(Table, ExistMember).

getCell(Table, RowKey, Column) ->
	case dets:lookup(Table, RowKey) of
		[Member] ->
			findColumn(Member#row.cells, Column);
		[] ->
			error
	end.

getRow(Table, RowKey) ->
	case dets:lookup(Table, RowKey) of
		[Member] ->
			Member;
		[] ->
			error
	end.


findColumn(RowData, Column) ->
	case lists:keyfind(Column, 2, lists:reverse(lists:keysort(4, RowData))) of
		false ->
			error;
		Tuple ->
			Tuple
	end.

	
			