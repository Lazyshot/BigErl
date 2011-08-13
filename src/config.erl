-module(config).

-import(log, [logError/1]).
-export([readConfig/1, getValue/2]).


readConfig(Conffile) ->
	Open = file:open(Conffile, read),
	case Open of
		
		{ok, Fd} ->
			Conf = parseFile(Fd),
			file:close(Fd),
			Conf;
		{error, Reason} ->
			log:logError(Reason)
	end.

parseLine(Line) ->
	LineVals = string:tokens(Line, "="),
	Option = list_to_atom(string:strip(lists:nth(1, LineVals))),
	Value = string:strip(lists:nth(2, LineVals)),
	[{Option, Value}].

parseFile(File, Conf) ->
	case file:read_line(File) of
		{ok, Data} ->
			lists:append(Conf, parseLine(Data));
		{error, Reason}->
			log:logError(Reason),
			Conf;
		eof ->
			Conf
	end.

parseFile(File) ->
	parseFile(File, []).

getDefaults() ->
	Config = [
			  {data_path, "data"}
			  ],
	Config.

getValue(Config, Option) ->
	case lists:keyfind(Option, 1, Config) of
		false ->
			Result = lists:keyfind(Option, 1, getDefaults());
		Tuple ->
			Result = element(2, Tuple)
	end,
	Result.
	