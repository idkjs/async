open Core;
open Poly;
open Async;
open Rpc;
module Limiter = Limiter_async.Token_bucket;

let run_at_limit = (~dispatch, msgs_per_sec, ()) => {
  let limiter = {
    let burst_size = Float.to_int(msgs_per_sec /. 1_000.);
    Limiter.create_exn(
      ~burst_size,
      ~sustained_rate_per_sec=msgs_per_sec,
      ~continue_on_error=false,
      ~initial_burst_size=burst_size,
      (),
    );
  };

  let event_precision = Scheduler.event_precision();
  let rec send_messages = () => {
    let ran = ref(false);
    Limiter.enqueue_exn(
      limiter,
      ~allow_immediate_run=true,
      1000,
      () => {
        for (_ in 2 to 1000) {
          dispatch();
        };
        ran := true;
      },
      (),
    );
    if (ran^) {
      send_messages();
    } else {
      let rec wait_for_previous_send = () =>
        if (ran^) {
          Deferred.unit;
        } else {
          Clock.after(event_precision) >>= wait_for_previous_send;
        };

      wait_for_previous_send() >>= send_messages;
    };
  };

  send_messages();
};

module Pipe_simple_test = {
  module String_pipe = {
    module Query = {
      [@deriving (bin_io, sexp)]
      type t' = {
        msg_size: Byte_units.Stable.V1.t,
        msgs_per_sec: int,
      };

      [@deriving (bin_io, sexp)]
      type t = option(t');

      let start = t => {
        let bytes =
          switch (t) {
          | None => 0
          | Some(t) => Byte_units.bytes_int_exn(t.msg_size)
          };

        let (reader, writer) = Pipe.create();
        let total_msgs = ref(0);
        let start = Time.now();
        Pipe.set_size_budget(reader, 1000);
        let string = String.init(bytes, ~f=_ => 'A');
        let stop = Pipe.closed(writer);
        Clock.every(~stop, Time.Span.of_sec(1.), () =>
          Log.Global.printf("Queue size: %d", Pipe.length(reader))
        );
        Clock.every(~stop, Time.Span.of_sec(1.), () =>
          Log.Global.printf(
            "Messages per sec: %f",
            Float.of_int(total_msgs^)
            /. Time.Span.to_sec(Time.diff(Time.now(), start)),
          )
        );
        let prev = ref(Time.now());
        let () =
          switch (t) {
          | None =>
            let rec loop = () =>
              Pipe.pushback(writer)
              >>> (
                () => {
                  Pipe.write_without_pushback(writer, string);
                  loop();
                }
              );

            loop();
          | Some(t) =>
            Clock.every'(
              ~stop,
              Time.Span.of_sec(1.),
              () => {
                let msgs = {
                  let new_time = Time.now();
                  let diff = Time.Span.to_sec(Time.diff(new_time, prev^));
                  Log.Global.printf("The diff is %f\n", diff);
                  prev := new_time;
                  Int.of_float(diff *. Int.to_float(t.msgs_per_sec));
                };

                if (!Pipe.is_closed(writer)) {
                  for (i in 1 to msgs) {
                    let _ = i;
                    incr(total_msgs);
                    Pipe.write_without_pushback(writer, string);
                  };
                };
                switch%map (Pipe.downstream_flushed(writer)) {
                | `Reader_closed
                | `Ok => ()
                };
              },
            )
          };

        reader;
      };

      let create = (msg_size, msgs_per_sec) => {msg_size, msgs_per_sec};
    };

    let rpc =
      Pipe_rpc.create(
        ~client_pushes_back=(),
        ~name="test-pipe-rpc",
        ~version=1,
        ~bin_query=Query.bin_t,
        ~bin_response=String.bin_t,
        ~bin_error=Nothing.bin_t,
        (),
      );
  };

  module Memory_consumption = {
    let init = () => {
      let major_cycles = ref(0);
      ignore(Gc.Alarm.create(() => incr(major_cycles)));
      Clock.every(Time.Span.of_sec(5.), () =>
        Log.Global.printf("%d major cycles", major_cycles^)
      );
    };
  };

  module Client = {
    let _is_the_right_string = (msg_size, string) =>
      String.length(string) == msg_size
      && String.for_all(string, ~f=(==)('A'));

    let main = (bytes, msgs_per_sec, host, port, ~rpc_impl, ()) => {
      Memory_consumption.init();
      let query =
        Option.map(msgs_per_sec, ~f=String_pipe.Query.create(bytes));
      let%bind connection =
        Rpc_impl.make_client(rpc_impl, host, port) >>| Result.ok_exn;
      switch%bind (
        Pipe_rpc.dispatch(String_pipe.rpc, connection, query)
        >>| Or_error.ok_exn
      ) {
      | Error(t) => Nothing.unreachable_code(t)
      | [@implicit_arity] Ok(pipe, _) =>
        let msgs = ref(0);
        let start = Time.now();
        let _msg_size = Byte_units.bytes_int_exn(bytes);
        Clock.every(
          Time.Span.of_sec(1.),
          () => {
            let now = Time.now();
            let secs = Time.Span.to_sec(Time.diff(now, start));
            Log.Global.printf(
              "%f msgs per sec",
              Float.of_int(msgs^) /. secs,
            );
          },
        );
        Pipe.iter_without_pushback(pipe, ~f=_string => incr(msgs));
      };
    };

    let command =
      Command.async_spec(
        ~summary="test client",
        Command.Spec.(
          empty
          +> flag("msg-size", required(Byte_units.arg_type), ~doc="")
          +> flag("msgs-per-sec", optional(int), ~doc="")
          +> flag("hostname", required(string), ~doc="")
          +> flag("port", required(int), ~doc="")
          ++ Rpc_impl.spec()
        ),
        main,
      );
  };

  module Server = {
    let implementation =
      Pipe_rpc.implement(String_pipe.rpc, ((), query) =>
        return(Ok(String_pipe.Query.start(query)))
      );

    let main = (port, ~rpc_impl, ()) => {
      Memory_consumption.init();
      let implementations =
        Implementations.create_exn(
          ~implementations=[implementation],
          ~on_unknown_rpc=`Raise,
        );

      let%bind _: Rpc_impl.Server.t =
        Rpc_impl.make_server(
          ~initial_connection_state=_ => (),
          ~implementations,
          ~port,
          rpc_impl,
        );

      Deferred.never();
    };

    let command =
      Command.async_spec(
        ~summary="test server",
        Command.Spec.(
          empty +> flag("port", required(int), ~doc="") ++ Rpc_impl.spec()
        ),
        main,
      );
  };

  let command =
    Command.group(
      ~summary=
        "Simple client and server to quickly check manually that pipe-rpc is working ok ",
      [("server", Server.command), ("client", Client.command)],
    );
};

module Heartbeat_pipe_test = {
  let main = () => {
    let implementations =
      Implementations.create_exn(~implementations=[], ~on_unknown_rpc=`Raise);

    let heartbeat_config =
      Connection.Heartbeat_config.create(
        ~timeout=Time_ns.Span.of_day(1.),
        ~send_every=Time_ns.Span.of_ms(1.),
        (),
      );

    let%bind server =
      Connection.serve(
        ~implementations,
        ~heartbeat_config,
        ~initial_connection_state=(_, _) => (),
        ~where_to_listen=Tcp.Where_to_listen.of_port_chosen_by_os,
        (),
      );

    let port = Tcp.Server.listening_on(server);
    Connection.with_client(
      Tcp.Where_to_connect.of_host_and_port({host: "127.0.0.1", port}),
      ~heartbeat_config,
      conn => {
        let counter = ref(0);
        let%bind () = Clock.after(sec(1.));
        Connection.add_heartbeat_callback(conn, () => incr(counter));
        [%test_eq: int](0, counter^);
        let%bind () = Clock_ns.after(Time_ns.Span.of_ms(100.));
        let c1 = counter^;
        [%test_pred: int](c1 => c1 > 0, c1);
        let%bind () = Clock_ns.after(Time_ns.Span.of_ms(100.));
        let c2 = counter^;
        [%test_pred: int](c2 => c2 > c1, c2);
        let%bind () = Tcp.Server.close(server);
        /* No more heartbeats now that the server is closed */
        Connection.add_heartbeat_callback(conn, () => assert(false));
        let%bind () = Clock_ns.after(Time_ns.Span.of_ms(100.));
        Deferred.unit;
      },
    )
    >>| Result.ok_exn;
  };

  let command =
    Command.async_spec(
      ~summary="test that heartbeat handlers are installed correctly",
      Command.Spec.empty,
      main,
    );
};

module Pipe_closing_test = {
  [@deriving bin_io]
  type query = [ | `Do_close | `Dont_close];

  let rpc =
    Pipe_rpc.create(
      (),
      ~name="pipe-closing-test",
      ~version=1,
      ~bin_query,
      ~bin_response=bin_unit,
      ~bin_error=Nothing.bin_t,
    );

  let main = () => {
    let implementations =
      Implementations.create_exn(
        ~on_unknown_rpc=`Raise,
        ~implementations=[
          Pipe_rpc.implement(
            rpc,
            ((), query) => {
              let pipe = fst(Pipe.create());
              switch (query) {
              | `Dont_close => ()
              | `Do_close => Pipe.close_read(pipe)
              };
              return(Ok(pipe));
            },
          ),
        ],
      );

    let%bind server =
      Connection.serve(
        (),
        ~implementations,
        ~initial_connection_state=(_, _) => (),
        ~where_to_listen=Tcp.Where_to_listen.of_port_chosen_by_os,
      );

    let port = Tcp.Server.listening_on(server);
    Connection.with_client(
      Tcp.Where_to_connect.of_host_and_port({host: "127.0.0.1", port}),
      conn => {
        let%bind (pipe, id) = Pipe_rpc.dispatch_exn(rpc, conn, `Dont_close);
        Pipe.close_read(pipe);
        let%bind reason = Pipe_rpc.close_reason(id);
        assert(reason == Closed_locally);
        let%bind (pipe, id) = Pipe_rpc.dispatch_exn(rpc, conn, `Do_close);
        let%bind reason = Pipe_rpc.close_reason(id);
        assert(Pipe.is_closed(pipe));
        assert(reason == Closed_remotely);
        let%bind (pipe, id) = Pipe_rpc.dispatch_exn(rpc, conn, `Dont_close);
        let%bind () = Connection.close(conn);
        let%bind reason = Pipe_rpc.close_reason(id);
        assert(Pipe.is_closed(pipe));
        assert(
          switch (reason) {
          | Error(_) => true
          | Closed_locally
          | Closed_remotely => false
          },
        );
        Deferred.unit;
      },
    )
    >>| Result.ok_exn;
  };

  let command =
    Command.async_spec(
      ~summary="test behavior of closing pipes",
      Command.Spec.empty,
      main,
    );
};

module Pipe_iter_test = {
  let rpc =
    Pipe_rpc.create(
      (),
      ~name="dispatch-iter-test",
      ~version=1,
      ~bin_query=Time.Span.bin_t,
      ~bin_response=Int.bin_t,
      ~bin_error=Nothing.bin_t,
    );

  let main = () => {
    let implementations =
      Implementations.create_exn(
        ~on_unknown_rpc=`Raise,
        ~implementations=[
          Pipe_rpc.implement(
            rpc,
            ((), query) => {
              let (r, w) = Pipe.create();
              don't_wait_for(
                Deferred.repeat_until_finished(0, counter =>
                  if (counter > 10) {
                    Pipe.close(w);
                    return(`Finished());
                  } else {
                    let%map () = Clock.after(query);
                    Pipe.write_without_pushback_if_open(w, counter);
                    `Repeat(counter + 1);
                  }
                ),
              );
              return(Ok(r));
            },
          ),
        ],
      );

    let%bind server =
      Connection.serve(
        (),
        ~implementations,
        ~initial_connection_state=(_, _) => (),
        ~where_to_listen=Tcp.Where_to_listen.of_port_chosen_by_os,
      );

    let port = Tcp.Server.listening_on(server);
    Connection.with_client(
      Tcp.Where_to_connect.of_host_and_port({host: "127.0.0.1", port}),
      conn => {
        let dispatch_exn = (query, f) =>
          switch%map (Pipe_rpc.dispatch_iter(rpc, conn, query, ~f)) {
          | Error(e) => Error.raise(e)
          | Ok(Error(nothing)) => Nothing.unreachable_code(nothing)
          | Ok(Ok(id)) => id
          };

        let next_expected: ref([ | `Update(int) | `Closed_remotely]) = (
          ref(`Update(0)): ref([ | `Update(int) | `Closed_remotely])
        );
        let finished = Ivar.create();
        let%bind _: Pipe_rpc.Id.t =
          dispatch_exn(
            Time.Span.millisecond,
            fun
            | Update(n) =>
              switch (next_expected^) {
              | `Update(n') =>
                assert(n == n');
                next_expected :=
                  (
                    if (n == 10) {
                      `Closed_remotely;
                    } else {
                      `Update(n + 1);
                    }
                  );
                Continue;
              | `Closed_remotely => assert(false)
              }
            | Closed(`By_remote_side) =>
              switch (next_expected^) {
              | `Update(_) => assert(false)
              | `Closed_remotely =>
                Ivar.fill(finished, ());
                Continue;
              }
            | Closed(`Error(e)) => Error.raise(e),
          );

        let%bind () = Ivar.read(finished);
        let finished = Ivar.create();
        let%bind id =
          dispatch_exn(
            Time.Span.second,
            fun
            | Update(_) => assert(false)
            | Closed(`By_remote_side) => {
                Ivar.fill(finished, ());
                Continue;
              }
            | Closed(`Error(e)) => Error.raise(e),
          );

        Pipe_rpc.abort(rpc, conn, id);
        let%bind () = Ivar.read(finished);
        let finished = Ivar.create();
        let%bind _: Pipe_rpc.Id.t =
          dispatch_exn(
            Time.Span.second,
            fun
            | Update(_)
            | Closed(`By_remote_side) => assert(false)
            | Closed(`Error(_)) => {
                Ivar.fill(finished, ());
                Continue;
              },
          );

        let%bind () = Connection.close(conn);
        Ivar.read(finished);
      },
    )
    >>| Result.ok_exn;
  };

  let command =
    Command.async_spec(
      ~summary="test behavior of dispatch_iter",
      Command.Spec.empty,
      main,
    );
};

module Pipe_direct_test = {
  let rpc =
    Pipe_rpc.create(
      ~name="test-pipe-direct",
      ~version=1,
      ~bin_query=[%bin_type_class: [ | `Close | `Expect_auto_close(int)]],
      ~bin_response=Int.bin_t,
      ~bin_error=Nothing.bin_t,
      (),
    );

  let main = () => {
    let auto_close_was_ok: array(Ivar.t(bool)) = (
      [|Ivar.create(), Ivar.create()|]: array(Ivar.t(bool))
    );
    let output = List.init(10, ~f=Fn.id);
    let impl =
      Pipe_rpc.implement_direct(
        rpc,
        ((), action, writer) => {
          List.iter(output, ~f=i =>
            switch (
              Pipe_rpc.Direct_stream_writer.write_without_pushback(writer, i)
            ) {
            | `Ok => ()
            | `Closed => assert(false)
            }
          );
          switch (action) {
          | `Close => Pipe_rpc.Direct_stream_writer.close(writer)
          | `Expect_auto_close(n) =>
            let ivar = auto_close_was_ok[n];
            upon(Clock.after(Time.Span.second), () =>
              Ivar.fill_if_empty(ivar, false)
            );
            upon(Pipe_rpc.Direct_stream_writer.closed(writer), () =>
              Ivar.fill_if_empty(ivar, true)
            );
          };
          return(Ok());
        },
      );

    let implementations =
      Implementations.create_exn(
        ~implementations=[impl],
        ~on_unknown_rpc=`Raise,
      );

    let%bind server =
      Connection.serve(
        ~implementations,
        ~initial_connection_state=(_, _) => (),
        ~where_to_listen=Tcp.Where_to_listen.of_port_chosen_by_os,
        (),
      );

    let port = Tcp.Server.listening_on(server);
    Connection.with_client(
      Tcp.Where_to_connect.of_host_and_port({host: "127.0.0.1", port}),
      conn => {
        let%bind (pipe, md) = Pipe_rpc.dispatch_exn(rpc, conn, `Close);
        let%bind l = Pipe.to_list(pipe);
        [%test_result: list(int)](l, ~expect=output);
        let%bind reason = Pipe_rpc.close_reason(md);
        assert(reason == Closed_remotely);
        let%bind (pipe, md) =
          Pipe_rpc.dispatch_exn(rpc, conn, `Expect_auto_close(0));
        let%bind result = Pipe.read_exactly(pipe, ~num_values=10);
        let l =
          switch (result) {
          | `Eof
          | `Fewer(_) => assert(false)
          | `Exactly(q) => Queue.to_list(q)
          };

        Pipe.close_read(pipe);
        [%test_result: list(int)](l, ~expect=output);
        let%bind reason = Pipe_rpc.close_reason(md);
        assert(reason == Closed_locally);
        let%bind was_ok = Ivar.read(auto_close_was_ok[0]);
        assert(was_ok);
        let%bind (pipe, md) =
          Pipe_rpc.dispatch_exn(rpc, conn, `Expect_auto_close(1));
        let%bind () = Connection.close(conn);
        let%bind l = Pipe.to_list(pipe);
        [%test_result: list(int)](l, ~expect=output);
        let%bind was_ok = Ivar.read(auto_close_was_ok[1]);
        assert(was_ok);
        switch%map (Pipe_rpc.close_reason(md)) {
        | Error(_) => ()
        | Closed_locally
        | Closed_remotely => assert(false)
        };
      },
    )
    >>| Result.ok_exn;
  };

  let command =
    Command.async_spec(
      ~summary="test behavior of implement_direct",
      Command.Spec.empty,
      main,
    );
};

module Pipe_rpc_performance_measurements = {
  module Protocol = {
    [@deriving bin_io]
    type query = float;
    [@deriving bin_io]
    type response = unit;

    let rpc =
      Pipe_rpc.create(
        ~client_pushes_back=(),
        ~name="test-rpc-performance",
        ~version=1,
        ~bin_query,
        ~bin_response,
        ~bin_error=Nothing.bin_t,
        (),
      );
  };

  module Client = {
    let main = (msgs_per_sec, host, port, ~rpc_impl, ()) => {
      let%bind connection =
        Rpc_impl.make_client(rpc_impl, host, port) >>| Result.ok_exn;
      switch%bind (
        Pipe_rpc.dispatch(Protocol.rpc, connection, msgs_per_sec)
        >>| Or_error.ok_exn
      ) {
      | Error(t) => Nothing.unreachable_code(t)
      | [@implicit_arity] Ok(pipe, _) =>
        let cnt = ref(0);
        let total_cnt = ref(0);
        let ratio_acc = ref(0.);
        let percentage_acc = ref(0.);
        let sample_to_collect_and_exit = ref(-5);
        don't_wait_for(
          Pipe.iter_without_pushback(
            Cpu_usage.samples(),
            ~f=percent => {
              let percentage = Percent.to_percentage(percent);
              incr(sample_to_collect_and_exit);
              if (percentage > 100.) {
                Print.printf(
                  "CPU pegged (%f). This test is not good.\n",
                  percentage,
                );
                Shutdown.shutdown(1);
              };
              if (sample_to_collect_and_exit^ == 10) {
                Print.printf(
                  "%f (cpu: %f)\n",
                  ratio_acc^ /. 10.,
                  percentage_acc^ /. 10.,
                );
                Shutdown.shutdown(0);
              } else if (sample_to_collect_and_exit^ >= 0) {
                if (cnt^ > 0) {
                  let ratio = percentage *. 1_000_000. /. Int.to_float(cnt^);
                  ratio_acc := ratio_acc^ +. ratio;
                  percentage_acc := percentage_acc^ +. percentage;
                };
              };
              cnt := 0;
            },
          ),
        );
        Pipe.iter'(
          pipe,
          ~f=queue => {
            let len = Queue.length(queue);
            cnt := cnt^ + len;
            total_cnt := total_cnt^ + len;
            Deferred.unit;
          },
        );
      };
    };
  };

  module Server = {
    let start_test = (~msgs_per_sec) => {
      let (reader, writer) = Pipe.create();
      don't_wait_for(
        run_at_limit(
          ~dispatch=Pipe.write_without_pushback(writer),
          msgs_per_sec,
          (),
        ),
      );
      reader;
    };

    let implementation =
      Pipe_rpc.implement(Protocol.rpc, ((), msgs_per_sec) =>
        return(Ok(start_test(~msgs_per_sec)))
      );
  };
};

module Rpc_performance_measurements = {
  [@deriving bin_io]
  type msg = unit;

  let one_way = One_way.create(~name="one-way-rpc", ~version=1, ~bin_msg);

  [@deriving bin_io]
  type query = unit;
  [@deriving bin_io]
  type response = unit;

  let rpc =
    Rpc.create(~name="regular-rpc", ~version=1, ~bin_query, ~bin_response);

  let run_client = (~dispatch, msgs_per_sec, host, port, ~rpc_impl, ()) => {
    let%bind connection =
      Rpc_impl.make_client(rpc_impl, host, port) >>| Result.ok_exn;
    run_at_limit(~dispatch=dispatch(connection), msgs_per_sec, ());
  };

  let one_way_client = (msgs_per_sec, host, port, ~rpc_impl, ()) =>
    run_client(
      ~dispatch=One_way.dispatch_exn(one_way),
      msgs_per_sec,
      host,
      port,
      ~rpc_impl,
      (),
    );

  let rpc_client = (msgs_per_sec, host, port, ~rpc_impl, ()) => {
    let dispatch = (connection, ()) =>
      don't_wait_for(Rpc.dispatch_exn(rpc, connection, ()));
    run_client(~dispatch, msgs_per_sec, host, port, ~rpc_impl, ());
  };

  module Server = {
    let cnt = ref(0);
    let one_way_implementation =
      One_way.implement(one_way, ((), ()) => incr(cnt));
    let rpc_implementation = Rpc.implement'(rpc, ((), ()) => incr(cnt));

    let main = (port, ~rpc_impl, ()) => {
      let implementations =
        Implementations.create_exn(
          ~implementations=[
            one_way_implementation,
            rpc_implementation,
            Pipe_rpc_performance_measurements.Server.implementation,
          ],
          ~on_unknown_rpc=`Raise,
        );

      let%bind _server =
        Rpc_impl.make_server(
          ~initial_connection_state=_ => (),
          ~implementations,
          ~port,
          rpc_impl,
        );

      let ratio_acc = ref(0.);
      let percentage_acc = ref(0.);
      let sample = ref(0);
      don't_wait_for(
        Pipe.iter_without_pushback(Cpu_usage.samples(), ~f=percent =>
          if (0 == cnt^) {
            ();
          } else {
            let percentage = Percent.to_percentage(percent);
            Print.printf(
              "%d %d %f (cpu: %f)\n",
              cnt^,
              sample^,
              ratio_acc^ /. Float.of_int(sample^),
              percentage_acc^ /. Float.of_int(sample^),
            );
            if (percentage > 100.) {
              Print.printf(
                "CPU pegged (%f). This test may not good.\n",
                percentage,
              );
            } else {
              if (sample^ >= 0) {
                if (cnt^ > 0) {
                  let ratio = percentage *. 1_000_000. /. Int.to_float(cnt^);
                  ratio_acc := ratio_acc^ +. ratio;
                  percentage_acc := percentage_acc^ +. percentage;
                };
              };
              cnt := 0;
            };
            incr(sample);
          }
        ),
      );
      Deferred.never();
    };
  };

  let server_command =
    Command.async_spec(
      ~summary="test server for one-way and regular rpcs",
      Command.Spec.(
        empty +> flag("port", required(int), ~doc="") ++ Rpc_impl.spec()
      ),
      Server.main,
    );

  let client_flags =
    Command.Spec.(
      empty
      +> flag("msg-per-sec", required(float), ~doc="")
      +> flag("hostname", required(string), ~doc="")
      +> flag("port", required(int), ~doc="")
      ++ Rpc_impl.spec()
    );

  let client_command =
    Command.group(
      ~summary="Clients",
      [
        (
          "one-way",
          Command.async_spec(
            ~summary="client for one-way rpc",
            client_flags,
            one_way_client,
          ),
        ),
        (
          "rpc",
          Command.async_spec(
            ~summary="client for regular rpc",
            client_flags,
            rpc_client,
          ),
        ),
        (
          "pipe",
          Command.async_spec(
            ~summary="client for pipe rpc",
            client_flags,
            Pipe_rpc_performance_measurements.Client.main,
          ),
        ),
      ],
    );
};

module Rpc_expert_test = {
  let rpc = (~name) =>
    Rpc.create(
      ~name,
      ~version=0,
      ~bin_query=bin_string,
      ~bin_response=bin_string,
    );

  /* names refer to how they're implemented */
  let unknown_raw_rpc = rpc(~name="unknown-raw");
  let raw_rpc = rpc(~name="raw");
  let normal_rpc = rpc(~name="normal");
  let custom_io_rpc_tag = "custom-io-rpc";
  let custom_io_rpc_version = 0;

  let raw_one_way_rpc =
    One_way.create(~name="raw-one-way", ~version=0, ~bin_msg=String.bin_t);

  let normal_one_way_rpc =
    One_way.create(~name="normal-one-way", ~version=0, ~bin_msg=String.bin_t);

  let the_query = "flimflam";
  let the_response = String.rev(the_query);

  let main = (debug, ~rpc_impl, ()) => {
    let level =
      if (debug) {
        `Debug;
      } else {
        `Error;
      };
    let log =
      Log.create(
        ~level,
        ~output=[Log.Output.stdout()],
        ~on_error=`Raise,
        (),
      );
    let (one_way_reader, one_way_writer) = Pipe.create();
    let assert_one_way_rpc_received = () =>
      switch%map (Pipe.read(one_way_reader)) {
      | `Eof => assert(false)
      | `Ok () => assert(Pipe.is_empty(one_way_reader))
      };

    let implementations = {
      let handle_raw = (responder, buf, ~pos as init_pos, ~len) => {
        let pos_ref = ref(init_pos);
        let query = String.bin_read_t(buf, ~pos_ref);
        [%test_result: string](query, ~expect=the_query);
        Log.debug(log, "query value = %S", query);
        assert(pos_ref^ - init_pos == len);
        let new_buf =
          Bin_prot.Utils.bin_dump(String.bin_writer_t, the_response);
        ignore(
          Rpc.Expert.Responder.schedule(
            responder,
            new_buf,
            ~pos=0,
            ~len=Bigstring.length(new_buf),
          ): [ | `Connection_closed | `Flushed(Deferred.t(unit))],
        );
      };

      let handle_unknown_raw =
          ((), ~rpc_tag, ~version, responder, buf, ~pos, ~len) => {
        Log.debug(log, "query: %s v%d", rpc_tag, version);
        assert(
          rpc_tag == Rpc.name(unknown_raw_rpc)
          && version == Rpc.version(unknown_raw_rpc),
        );
        try(
          {
            handle_raw(responder, buf, ~pos, ~len);
            Deferred.unit;
          }
        ) {
        | e =>
          Log.debug(log, "got exception: %{Exn#mach}"^, e);
          Rpc.Expert.Responder.write_error(responder, Error.of_exn(e));
          Deferred.unit;
        };
      };

      Implementations.Expert.create_exn(
        ~implementations=[
          Rpc.implement(
            normal_rpc,
            ((), query) => {
              [%test_result: string](query, ~expect=the_query);
              return(the_response);
            },
          ),
          Rpc.Expert.implement'(
            raw_rpc,
            ((), responder, buf, ~pos, ~len) => {
              handle_raw(responder, buf, ~pos, ~len);
              Replied;
            },
          ),
          Rpc.Expert.implement_for_tag_and_version'(
            ~rpc_tag=custom_io_rpc_tag,
            ~version=custom_io_rpc_version,
            ((), responder, buf, ~pos, ~len) => {
              handle_raw(responder, buf, ~pos, ~len);
              Replied;
            },
          ),
          One_way.implement(
            normal_one_way_rpc,
            ((), query) => {
              Log.debug(
                log,
                "received one-way RPC message (normal implementation)",
              );
              Log.debug(log, "message value = %S", query);
              [%test_result: string](query, ~expect=the_query);
              Pipe.write_without_pushback(one_way_writer, ());
            },
          ),
          One_way.Expert.implement(
            raw_one_way_rpc,
            ((), buf, ~pos, ~len) => {
              Log.debug(
                log,
                "received one-way RPC message (expert implementation)",
              );
              let pos_ref = ref(pos);
              let query = String.bin_read_t(buf, ~pos_ref);
              Log.debug(log, "message value = %S", query);
              assert(pos_ref^ - pos == len);
              [%test_result: string](query, ~expect=the_query);
              Pipe.write_without_pushback(one_way_writer, ());
            },
          ),
        ],
        ~on_unknown_rpc=`Expert(handle_unknown_raw),
      );
    };

    let%bind server =
      Rpc_impl.make_server(
        ~implementations,
        ~initial_connection_state=_ => (),
        rpc_impl,
      );

    let port = Rpc_impl.Server.bound_on(server);
    let%bind result =
      Rpc_impl.with_client(
        rpc_impl,
        "127.0.0.1",
        port,
        conn => {
          let%bind () =
            Deferred.List.iter(
              [unknown_raw_rpc, raw_rpc, normal_rpc],
              ~f=rpc => {
                Log.debug(log, "sending %s query normally", Rpc.name(rpc));
                let%bind response = Rpc.dispatch_exn(rpc, conn, the_query);
                Log.debug(log, "got response");
                [%test_result: string](response, ~expect=the_response);
                let buf =
                  Bin_prot.Utils.bin_dump(String.bin_writer_t, the_query);
                Log.debug(
                  log,
                  "sending %s query via Expert interface",
                  Rpc.name(rpc),
                );
                let%map response =
                  Deferred.create(i =>
                    ignore(
                      Rpc.Expert.schedule_dispatch(
                        conn,
                        ~rpc_tag=Rpc.name(rpc),
                        ~version=Rpc.version(rpc),
                        buf,
                        ~pos=0,
                        ~len=Bigstring.length(buf),
                        ~handle_error=e => Ivar.fill(i, Error(e)),
                        ~handle_response=
                          (buf, ~pos, ~len) => {
                            let pos_ref = ref(pos);
                            let response = String.bin_read_t(buf, ~pos_ref);
                            assert(pos_ref^ - pos == len);
                            Ivar.fill(i, Ok(response));
                            Deferred.unit;
                          },
                      ): [
                         | `Connection_closed
                         | `Flushed(Deferred.t(unit))
                       ],
                    )
                  );

                Log.debug(log, "got response");
                [%test_result: Or_error.t(string)](
                  response,
                  ~expect=Ok(the_response),
                );
              },
            );

          let%bind () = {
            let buf = Bin_prot.Utils.bin_dump(String.bin_writer_t, the_query);
            Log.debug(
              log,
              "sending %s query via Expert interface",
              custom_io_rpc_tag,
            );
            let%map response =
              Deferred.create(i =>
                ignore(
                  Rpc.Expert.schedule_dispatch(
                    conn,
                    ~rpc_tag=custom_io_rpc_tag,
                    ~version=custom_io_rpc_version,
                    buf,
                    ~pos=0,
                    ~len=Bigstring.length(buf),
                    ~handle_error=e => Ivar.fill(i, Error(e)),
                    ~handle_response=
                      (buf, ~pos, ~len) => {
                        let pos_ref = ref(pos);
                        let response = String.bin_read_t(buf, ~pos_ref);
                        assert(pos_ref^ - pos == len);
                        Ivar.fill(i, Ok(response));
                        Deferred.unit;
                      },
                  ): [ | `Connection_closed | `Flushed(Deferred.t(unit))],
                )
              );

            Log.debug(log, "got response");
            [%test_result: Or_error.t(string)](
              response,
              ~expect=Ok(the_response),
            );
          };

          Deferred.List.iter(
            [raw_one_way_rpc, normal_one_way_rpc],
            ~f=rpc => {
              Log.debug(log, "sending %s query normally", One_way.name(rpc));
              One_way.dispatch_exn(rpc, conn, the_query);
              let%bind () = assert_one_way_rpc_received();
              Log.debug(
                log,
                "sending %s query via Expert.dispatch",
                One_way.name(rpc),
              );
              let buf =
                Bin_prot.Utils.bin_dump(String.bin_writer_t, the_query);
              let pos = 0;
              let len = Bigstring.length(buf);
              switch (One_way.Expert.dispatch(rpc, conn, buf, ~pos, ~len)) {
              | `Ok => ()
              | `Connection_closed => assert(false)
              };
              let%bind () = assert_one_way_rpc_received();
              Log.debug(
                log,
                "sending %s query via Expert.schedule_dispatch",
                One_way.name(rpc),
              );
              let%bind () =
                switch (
                  One_way.Expert.schedule_dispatch(rpc, conn, buf, ~pos, ~len)
                ) {
                | `Flushed(f) => f
                | `Connection_closed => assert(false)
                };

              assert_one_way_rpc_received();
            },
          );
        },
      );

    Result.ok_exn(result);
    Rpc_impl.Server.close(server);
  };

  let command =
    Command.async_spec(
      ~summary="connect basic and low-level clients",
      Command.Spec.(
        empty +> flag("debug", no_arg, ~doc="") ++ Rpc_impl.spec()
      ),
      main,
    );
};

module Connection_closing_test = {
  let one_way_unimplemented =
    One_way.create(~name="unimplemented", ~version=1, ~bin_msg=bin_unit);

  let never_returns =
    Rpc.create(
      ~name="never-returns",
      ~version=1,
      ~bin_query=bin_unit,
      ~bin_response=bin_unit,
    );

  let never_returns_impl =
    Rpc.implement(never_returns, ((), ()) => Deferred.never());

  let implementations =
    Implementations.create_exn(
      ~implementations=[never_returns_impl],
      ~on_unknown_rpc=`Continue,
    );

  let main = () => {
    let most_recent_server_conn = ref(None);
    let%bind server =
      Connection.serve(
        ~implementations,
        ~initial_connection_state=
          (_, conn) => most_recent_server_conn := Some(conn),
        ~where_to_listen=Tcp.Where_to_listen.of_port_chosen_by_os,
        (),
      );

    let port = Tcp.Server.listening_on(server);
    let connect = () =>
      Connection.client(
        Tcp.Where_to_connect.of_host_and_port({host: "127.0.0.1", port}),
      )
      >>| Result.ok_exn;

    let dispatch_never_returns = conn => {
      let response = Rpc.dispatch(never_returns, conn, ());
      let%bind () = Clock.after(Time.Span.second);
      assert(!Deferred.is_determined(response));
      return(response);
    };

    let check_response_is_error = (here, conn, response_deferred) =>
      switch%bind (
        Clock.with_timeout(Time.Span.second, Connection.close_finished(conn))
      ) {
      | `Timeout =>
        failwithf(
          "%{Source_code_position} timed out waiting for connection to close"^,
          here,
          (),
        )
      | `Result () =>
        switch%map (Clock.with_timeout(Time.Span.second, response_deferred)) {
        | `Timeout =>
          failwithf(
            "%{Source_code_position} timed out waiting for response to be determined"
              ^,
            here,
            (),
          )
        | `Result(Ok ()) =>
          failwithf(
            "%{Source_code_position} somehow got an ok response for RPC"^,
            here,
            (),
          )
        | `Result(Error(_)) => ()
        }
      };

    /* Kill the connection after dispatching the RPC. */
    let%bind conn = connect();
    let%bind response_deferred = dispatch_never_returns(conn);
    let server_conn =
      Option.value_exn(~here=[%here], most_recent_server_conn^);
    let%bind () = Connection.close(server_conn);
    let%bind () = check_response_is_error([%here], conn, response_deferred);
    /* Call an unknown one-way RPC while the connection is open. This causes somewhat
       strange but not problematic behavior -- the server sends back an "unknown RPC"
       message, but the client doesn't have a response handler installed, so it closes the
       connection. */
    let%bind conn = connect();
    let%bind response_deferred = dispatch_never_returns(conn);
    One_way.dispatch_exn(one_way_unimplemented, conn, ());
    check_response_is_error([%here], conn, response_deferred);
  };

  let command =
    Command.async(
      ~summary=
        "test that responses are determined when connections are closed",
      Command.Param.return(main),
    );
};

let all_regression_tests =
  Command.async_spec(
    ~summary="run all regression tests",
    Command.Spec.(empty +> flag("debug", no_arg, ~doc="") ++ Rpc_impl.spec()),
    (debug, ~rpc_impl, ()) => {
      let%bind () = Heartbeat_pipe_test.main();
      let%bind () = Pipe_closing_test.main();
      let%bind () = Pipe_iter_test.main();
      let%bind () = Pipe_direct_test.main();
      let%bind () = Rpc_expert_test.main(debug, ~rpc_impl, ());
      Connection_closing_test.main();
    },
  );

let () =
  Command_unix.run(
    Command.group(
      ~summary="Various tests for rpcs",
      [
        (
          "performance",
          Command.group(
            ~summary="Plain rpc performance test",
            [
              ("server", Rpc_performance_measurements.server_command),
              ("client", Rpc_performance_measurements.client_command),
            ],
          ),
        ),
        (
          "pipe",
          Command.group(
            ~summary="Pipe rpc",
            [
              ("simple", Pipe_simple_test.command),
              ("closing", Pipe_closing_test.command),
              ("iter", Pipe_iter_test.command),
              ("direct", Pipe_direct_test.command),
            ],
          ),
        ),
        (
          "expert",
          Command.group(
            ~summary="Testing Expert interfaces",
            [("test", Rpc_expert_test.command)],
          ),
        ),
        (
          "heartbeat",
          Command.group(
            ~summary="Testing heartbeats",
            [("test-heartbeat-callback", Heartbeat_pipe_test.command)],
          ),
        ),
        ("regression", all_regression_tests),
        ("connection-inspector", Rpc_connection_inspector.command),
        ("connection-close", Connection_closing_test.command),
      ],
    ),
  );
