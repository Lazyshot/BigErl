-module(log).

-export([writeMsg/2, printMsg/2, logError/1, logInfo/1, logDebug/1]).
-import(erlang, [localtime/0]).

writeMsg(Level, Text) ->
	case file:open("error.log", [append]) of
		{ok, Fd} ->
			{{Year, Month, Day}, {Hour, Minute, Second}} = localtime(),
			io:format(Fd, "~p-~p-~p ~p:~p:~p <~p>: ~p~n", [Month, Day, Year, Hour, Minute, Second, Level, Text]);
		Error ->
			io:format("Error: ~p~n", [Error])
	end.
		

printMsg(Level, Text) ->
	{{Year, Month, Day}, {Hour, Minute, Second}} = localtime(),
	io:format("~p-~p-~p ~p:~p:~p <~p>: ~p~n", [Month, Day, Year, Hour, Minute, Second, Level, Text]).

  
logError(Error) ->
	writeMsg(error, Error),
	printMsg(error, Error).

logInfo(Info) ->
	writeMsg(info, Info),
	printMsg(info, Info).

logDebug(Debug) ->
	%writeMsg(debug, Debug),
	printMsg(debug, Debug).	