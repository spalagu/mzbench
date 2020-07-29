-module(tcpshort_worker).

-export([initial_state/0,
         metrics/0,
         connect_and_send/3]).

-define(Options, [
    binary,
    {packet, 0}
]).

-define(Timeout, 5000).
-define(WaitPollTime, 100).

-record(s, {
    socket = undefined,
    sender = undefined
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

connect_and_send(Host, Port, Message) ->
  {E, Socket} = gen_tcp:connect(Host, Port, ?Options),
  case E of
      ok -> mzb_metrics:notify({"connect.ok", counter}, 1),
            mzb_metrics:notify({"connect.current", counter}, 1),
            {E, Reason} = gen_tcp:send(Socket, Message),
            case E of
                ok -> mzb_metrics:notify({"request.ok", counter}, 1),
                      gen_tcp:close(Socket),
                      mzb_metrics:notify({"connect.current", counter}, -1);
                E -> lager:error("Request sync error: ~p", [Reason]),
                     mzb_metrics:notify({"request.error", counter}, 1),
                     gen_tcp:close(Socket),
                     mzb_metrics:notify({"connect.current", counter}, -1)
            end;
      E -> lager:error("connect error: ~p", [Socket]),
            mzb_metrics:notify({"request.error", counter}, 1)
  end.