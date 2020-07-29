-module(udp_worker).

-export([initial_state/0,
         metrics/0,
         connect_sync/2,
         request_sync/3,
         send_n_get_sync/2,
         close_sync/2]).

-define(Options, [
    binary,
    {active, false},
    {buffer, 65536},
    {reuseaddr, true}
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
            {graph, #{title => "Latencies",
                      units => "msec",
                      metrics => [{"latency", histogram}]}}
        ]}
    ].

initial_state() -> #s{}.


%% The following functions are synchronous
%% They are not recommended to use if you have big difference between percentile levels
%% due to coordinated omission problem

connect_sync(State, _Meta) ->
    {ok, Socket} = gen_udp:open(0, ?Options),
    {nil, State#s{socket = Socket}}.

request_sync(State, Meta, Host, Port, Message) when is_list(Message) ->
  request_sync(State, Meta, Host, Port, list_to_binary(Message));
request_sync(#s{socket = Socket} = State, _Meta, Host, Port, Message) ->
  {Latency, E} = timer:tc(?MODULE, send_n_get_sync, [Socket, Host, Port, Message]),
  mzb_metrics:notify({"latency", histogram}, Latency div 1000),
  case E of
      ok -> mzb_metrics:notify({"request.ok", counter}, 1);
      E -> lager:error("Request sync error: ~p", [E]),
           mzb_metrics:notify({"request.error", counter}, 1)
  end,
  {nil, State}.

send_n_get_sync(Socket, Host, Port, Message) ->
  case gen_udp:send(Socket, Host, Port, Message) of
      {ok, _Binary} -> ok;
      E -> E
  end.

close_sync(#s{socket = Socket} = State, _Meta) ->
  if Socket =/= undefined -> gen_udp:close(Socket); true -> ok end.