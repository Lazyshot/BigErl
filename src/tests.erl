-module(tests).


-export([createAndMutate/1, fsync/3, benchmarks/2]).


print(Result) ->
	io:format("~p~n", [Result]).

createAndMutate(Name) ->
	print("Test create region"),
	Tables = hashtable:createTable(Name, []),
	print(Tables),
	print("Test insert record"),
	Tables2 = hashtable:mutateRow(Tables, Name, "bryan", [{mutation, "info:name", "bryan", 0}]),
	print(Tables2),
	print("Test second insert"),
	Tables3 = hashtable:mutateRow(Tables2, Name, "jimmy", [{mutation, "info:name", "jimmy", 0}]),
	print(Tables3),
	print("Test Overwriting"),
	Tables4 = hashtable:mutateRow(Tables3, Name, "jimmy", [{mutation, "info:name", "james", 0}]),
	print(Tables4),
	print("Test Sorting"),
	Tables5 = hashtable:mutateRow(Tables4, Name, "jim", [{mutation, "info:name", "james", 0}]),
	print(Tables5),
	print("Test Getting"),
	Row4 = hashtable:getRow(hashtable:getTable(Tables5, Name), "jim"),
	print(Row4),
	Tables5.

fsync(File, Region, RegionName) ->
	fsync:saveRegion(File, Region),
	fsync:loadRegion(File, RegionName).

benchmarks(Name, Limit) ->
	log:logInfo("Start"),
	Tables = hashtable:createTable(Name, []),
	benchmarks_run(Tables, Name, 0, Limit),
	log:logInfo("Done").

benchmarks_run(Tables, Name, N, Limit) ->
	NTables = hashtable:mutateRow(Tables, Name, N, [{mutation, "info:id", N, 0}]),
	
	if 
		N > Limit ->
			done;
		true ->
			benchmarks_run(NTables, Name, N+1, Limit)
	end.
	