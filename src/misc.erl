-module(misc).

-export([getTimestamp/0]).

getTimestamp() ->
	{Mega, Secs, _} = now(),
	Mega*1000000 + Secs.