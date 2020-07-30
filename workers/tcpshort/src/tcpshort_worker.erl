-module(tcpshort_worker).

-export([initial_state/0,
         metrics/0,
         connect/4,
         request/3]).

-define(Options, [
    binary,
    {packet, 0}
]).

-define(Timeout, 5000).
-define(WaitPollTime, 100).

-record(s, {
    socket = undefined
}).

metrics() ->
    [
        {group, "Summary", [
            {graph, #{title => "Requests",
                      metrics => [{"request.ok", counter}, {"request.error", counter}]}},
            {graph, #{title => "Connects",
                      metrics => [{"connect.ok", counter}, {"connect.error", counter}, {"connect.current", counter}]}}
        ]}
    ].

initial_state() -> #s{}.

connect(State, _Meta, Host, Port) ->
    {Result, Socket} = gen_tcp:connect(Host, Port, ?Options),
    case Result of
        ok -> mzb_metrics:notify({"connect.ok", counter}, 1),
              mzb_metrics:notify({"connect.current", counter}, 1);
        error -> lager:error("connect error: ~p", [Socket]),
             mzb_metrics:notify({"connect.error", counter}, 1)
    end,
    {nil, State#s{socket = Socket}}.

request(State, Meta, Message) when is_list(Message) ->
  request(State, Meta, list_to_binary(Message));
request(#s{socket = Socket} = State, _Meta, Message) ->
  if Socket =/= undefined -> 
    Result = gen_tcp:send(Socket, Message),
    case Result of
        ok -> mzb_metrics:notify({"request.ok", counter}, 1),
                gen_tcp:close(Socket),
                mzb_metrics:notify({"connect.current", counter}, -1);
        {error, Reason} -> lager:error("Request sync error: ~p", [Reason]),
            mzb_metrics:notify({"request.error", counter}, 1),
            gen_tcp:close(Socket),
            mzb_metrics:notify({"connect.current", counter}, -1)
    end
  end,
  {nil, State}.
