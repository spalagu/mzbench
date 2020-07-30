-module(udp_worker).

-export([initial_state/0,
         metrics/0,
         connect/2,
         request/5]).

-define(Options, [
    binary
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
            {graph, #{title => "Current",
                      metrics => [{"socketOpen.current", counter}]}},
            {graph, #{title => "socketOpen",
                      metrics => [{"socketOpen.ok", counter}, {"socketOpen.error", counter}]}}
        ]}
    ].

initial_state() -> #s{}.

connect(State, _Meta) ->
    {Result, Socket} = gen_udp:open(0, ?Options),
    case Result of
        ok -> mzb_metrics:notify({"socketOpen.ok", counter}, 1),
              mzb_metrics:notify({"socketOpen.current", counter}, 1);
        error -> lager:error("socketOpen error: ~p", [Socket]),
             mzb_metrics:notify({"socketOpen.error", counter}, 1)
    end,
    {nil, State#s{socket = Socket}}.

request(State, Meta, Host, Port, Message) when is_list(Message) ->
  request(State, Meta, Host, Port, list_to_binary(Message));
request(#s{socket = Socket} = State, _Meta, Host, Port, Message) ->
  if Socket =/= undefined -> 
    Result = gen_udp:send(Socket, Host, Port, Message),
    case Result of
        ok -> mzb_metrics:notify({"request.ok", counter}, 1),
                gen_udp:close(Socket),
                mzb_metrics:notify({"socketOpen.current", counter}, -1);
        {error, Reason} -> lager:error("Request sync error: ~p", [Reason]),
            mzb_metrics:notify({"request.error", counter}, 1),
            gen_udp:close(Socket),
            mzb_metrics:notify({"socketOpen.current", counter}, -1)
    end
  end,
  {nil, State}.
