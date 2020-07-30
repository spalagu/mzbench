-module(tcplong_worker).

-export([initial_state/0,
         metrics/0,
         connect/4,
         request/3]).

-define(Options, [
    binary,
    {active, false},
    {buffer, 65536},
    {keepalive, true},
    {reuseaddr, true}
]).

-define(Timeout, 5000).
-define(WaitPollTime, 100).

-record(s, {
    socket = undefined,
    host = undefined,
    port = undefined
}).

metrics() ->
    [
        {group, "Summary", [
            {graph, #{title => "Current",
                      metrics => [{"connect.current", counter}]}},
            {graph, #{title => "Reconnects",
                      metrics => [{"reconnect", counter}]}},
            {graph, #{title => "Requests",
                      metrics => [{"request.ok", counter}, {"request.error", counter}]}},
            {graph, #{title => "Connects",
                      metrics => [{"connect.ok", counter}, {"connect.error", counter}]}}
        ]}
    ].

initial_state() -> #s{}.

connect(State, _Meta, Host, Port) ->
    {Result, Socket} = gen_tcp:connect(Host, Port, ?Options),
    case Result of
        ok -> mzb_metrics:notify({"connect.ok", counter}, 1);
        error -> lager:error("connect error: ~p", [Socket]),
             mzb_metrics:notify({"connect.error", counter}, 1)
    end,
    {nil, State#s{socket = Socket, host = Host, port = Port}}.

request(State, Meta, Message) when is_list(Message) ->
  request(State, Meta, list_to_binary(Message));
request(#s{socket = Socket, host = Host, port = Port} = State, _Meta, Message) ->
  Socket2 = if Socket == undefined -> 
    connect(State, _Meta, Host, Port), mzb_metrics:notify({"reconnect", counter}, 1); true -> Socket end,
  Result = gen_tcp:send(Socket2, Message),
  case Result of
      ok -> mzb_metrics:notify({"request.ok", counter}, 1);
      {error, Reason} -> lager:error("Request sync error: ~p", [Reason]),
           mzb_metrics:notify({"request.error", counter}, 1)
  end,
  {nil, State}.