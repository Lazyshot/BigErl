-module(fsync).

-export([loadRegion/2, loadRegion/4, parseLine/1, saveRegion/2, loadRegionDisk/1, readSingleLine/2, writeSingleLine/4, appendSingleLine/3]).

-record(region, {name, table, data, start="", stop="", inmemory=0, file, fd=false}).


%% Load Region into memory
loadRegion(RegionFile, Name) ->
	case file:open(RegionFile, [read, binary]) of
		{ok, Fd} -> 
			loadRegion(RegionFile, Name, Fd, []);
		{error, Reason} ->
			{error, Reason}
	end.

loadRegion(RegionFile, Name, Fd, Acc) ->
	case file:read_line(Fd) of 
		{ok, Data} ->
			loadRegion(RegionFile, Name, Fd, lists:append([Acc, [parseLine(Data)]]));
		eof ->
			Tree = gb_trees:from_orddict(Acc),
			gb_trees:balance(Tree);
		{error, Reason} ->
			{error, Reason}
	end.


%% Load map of row keys to disk locations

loadRegionDisk(Region) ->
	Name = Region#region.name,
	RegionFile = Region#region.file,
	
	if
		Region#region.fd =:= false ->
			case file:open(RegionFile, [read,write,binary]) of
				{ok, Fd} -> 
					Region#region{data=loadRegionDisk(RegionFile, Name, 0, Fd, [])};
				{error, Reason} ->
					{error, Reason}
			end;
		true ->
			file:position(Region#region.fd, bof),
			D = (catch loadRegionDisk(RegionFile, Name,0 , Region#region.fd, [])),
			log:logDebug(D),
			Region#region{data=D}
	end.

loadRegionDisk(RegionFile, _Name, Pos, Fd, Acc) ->	
	case file:read_line(Fd) of
		
		{ok, TData} ->
			Data = binary:part(TData,{0 , byte_size(TData) - 1}),
			NPos = Pos + length(binary_to_list(Data)) + 1,
			
			case parseLine(Data) of
				
				{'EXIT', _Reason} ->
					%log:logError({error, Reason}),
					loadRegionDisk(RegionFile, "", NPos,  Fd, Acc);
				
				{RowKey, _Cells} ->
					RowMetaInfo = {RowKey, NPos},
					loadRegionDisk(RegionFile, "", NPos, Fd, [RowMetaInfo | Acc]);
				
				_Else ->
					%log:logError({error, {malformeddata, Data}}),
					loadRegionDisk(RegionFile, "", NPos,  Fd, Acc)
			
			end;
		
		eof ->
			%log:logDebug("here?"),
			
			gb_trees:from_orddict(Acc);
			
			%log:logDebug("here?");
		
		{error, Reason} ->
			%log:logError({error, Reason}),
			case file:open(RegionFile, [read, write ,binary]) of
				{ok, NFd} -> 
					case file:position(NFd, Pos) of
						{ok, NPos} ->
							loadRegionDisk(RegionFile, "", NPos, NFd, Acc);
						{error, Reason} ->
							log:logError({error, Reason})
					end;
				{error, Reason} ->
					log:logError({error, Reason})
			end
	end.

%% Read a single line of a data file for a get or mutation
readSingleLine(Region, Pos) ->
	if
		Region#region.fd == false ->
			case file:open(Region#region.file, [read,write,binary]) of
				{ok, TFd} -> 
					Fd = TFd;
				{error, TReason} ->
					Fd = false,
					log:logError({error, TReason})
			end,
			NRegion = Region#region{fd=Fd};
		true ->
			Fd = Region#region.fd,
			NRegion = Region
	end,
	case file:position(Fd, Pos) of
		{ok, _NPos} ->
			case file:read_line(Fd) of
				{ok, Data} ->
					{NRegion, parseLine(Data)};
				eof ->
					{error, "End of file, unexpected. malformed metadata or file corrupted"};
				{error, Reason} ->
					log:logError({error, Reason})
			end;
		{error, Reason} ->
			log:logError({error, Reason})
	end.



%% Write to a single line of a file, given a position
writeSingleLine(Region, Pos, Key, Data) ->
	if
		Region#region.fd == false ->
			case file:open(Region#region.file, [read,write,binary]) of
				{ok, TFd} -> 
					Fd = TFd;
				{error, TReason} ->
					Fd = false,
					log:logError({error, TReason})
			end,
			NRegion = Region#region{fd=Fd};
		true ->
			Fd = Region#region.fd,
			NRegion = Region
	end,
	case file:position(Fd, Pos) of
		{ok, _NPos} ->
			case file:write(Fd, term_to_binary({Key, Data})) of
				ok ->
					{NRegion, success};
				{error, Reason} ->
					log:logError({error, Reason})
			end;
		{error, Reason} ->
			log:logError({error, Reason})
	end.


	
appendSingleLine(Region, Key, Data) ->
	E1 = list_to_binary([10]),
	E2 = term_to_binary({Key, Data}),
	QFd = Region#region.fd,
	RegionFile = Region#region.file,
	
	if
		QFd == false ->
			case file:open(RegionFile, [read,write,binary]) of
				{ok, TFd} -> 
					Fd = TFd;
				{error, TReason} ->
					Fd = false,
					log:logError({error, TReason})
			end,
			NRegion = Region#region{fd=Fd};
		true ->
			Fd = Region#region.fd,
			NRegion = Region
	end,
	
	case file:position(Fd, eof) of
		{ok, NPos} ->
			case file:write(Fd,  << E2/binary, E1/binary >>) of
				ok ->
					{NRegion, NPos};
				{error, Reason} ->
					log:logError({error, Reason})
			end;
		{error, Reason} ->
			log:logError({error, Reason})
	end.

parseLine(Data) ->
	catch binary_to_term(Data).


saveRegion(RegionFile, RegionData) ->
	case file:open(RegionFile, [write, binary]) of
		{ok, Fd} ->
			if
				is_list(RegionData) ->
					saveRegion(RegionFile, RegionData, Fd);
				true ->
					saveRegion(RegionFile, gb_trees:to_list(RegionData), Fd)
			end;			
		{error, Reason} ->
			{error, Reason}
	end.

saveRegion(RegionFile, [], _Fd) ->
	RegionFile;
saveRegion(RegionFile, [Row | Rest], Fd) ->
	E1 = list_to_binary([10]),
	E2 = term_to_binary(Row),
	
	case file:write(Fd, <<E2/binary, E1/binary>>) of
		ok ->
			saveRegion(RegionFile, Rest, Fd);
		{error, Reason} ->
			{error, Reason}
	end.
	