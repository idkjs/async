open! Core;
open Jane;
open! Async;
open Rpc;

let () =
  if (Time.Span.(>)(Scheduler.event_precision(), Time.Span.of_us(1.))) {
    Core.eprintf(
      {|%s: you need to run this program with:

  ASYNC_CONFIG='((timing_wheel_config ((alarm_precision 1us))))'

Otherwise the async timing wheel won't be precise enough.
|},
      Filename.basename(Sys.executable_name),
    );
    Caml.exit(2);
  };

module Spec = {
  open Command.Spec;

  let port = () =>
    step((main, port) => main(~port))
    +> flag(
         "-port",
         optional_with_default(12345, int),
         ~doc=" RPC server port",
       );

  let host = () =>
    step((main, host) => main(~host))
    +> flag("-host", required(string), ~doc=" RPC server hostname");
};

module Stats = {
  type t = {
    mutable count: int,
    mutable total: Time_ns.Span.t,
    mutable max: Time_ns.Span.t,
    mutable min: Time_ns.Span.t,
    mutable count_by_us: array(int),
  };

  let create = () => {
    count: 0,
    total: Time_ns.Span.zero,
    max: Time_ns.Span.zero,
    min: Time_ns.Span.max_value_for_1us_rounding,
    count_by_us: Array.create(0, ~len=1000),
  };

  let add = (t, ~span) => {
    t.count = t.count + 1;
    t.total = Time_ns.Span.(+)(t.total, span);
    if (Time_ns.Span.(<)(span, t.min)) {
      t.min = span;
    };
    if (Time_ns.Span.(>)(span, t.max)) {
      t.max = span;
    };
    let us = Time_ns.Span.to_int_ns(span) / 1000;
    if (us < Array.length(t.count_by_us)) {
      t.count_by_us[us] = t.count_by_us[us] + 1;
    };
  };

  let print = t => {
    open Core;
    let (rows, cols) =
      try(ok_exn(Linux_ext.get_terminal_size, `Controlling)) {
      | _ => (80, 25)
      };

    let histogram_height = rows - 8;
    let start = {
      let rec search = i =>
        if (i >= Array.length(t.count_by_us) - cols || t.count_by_us[i] > 0) {
          i / 10 * 10;
        } else {
          search(i + 1);
        };

      search(0);
    };

    let cols = Int.min(Array.length(t.count_by_us) - start, cols);
    let count_by_us = Array.sub(t.count_by_us, ~pos=start, ~len=cols);
    let count_displayed = Array.fold(count_by_us, ~init=0, ~f=(+));
    if (histogram_height > 0 && count_displayed > 0) {
      let displayed_max = Array.fold(count_by_us, ~init=0, ~f=Int.max);
      let histo =
        Array.map(count_by_us, ~f=n => n * histogram_height / displayed_max);

      let lines =
        Array.init(histogram_height, ~f=row =>
          String.init(cols, ~f=col =>
            if (row < histo[col]) {
              '#';
            } else {
              ' ';
            }
          )
        );

      for (i in histogram_height - 1 downto 0) {
        print_string(lines[i]);
        Out_channel.output_char(Out_channel.stdout, '\n');
      };
      let col = ref(0);
      while (col^ < cols) {
        let next = col^ + 10;
        let s = Int.to_string(start + col^);
        let len = String.length(s);
        if (col^ + len <= cols) {
          print_string(s);
          print_string(String.make(10 - len, ' '));
        };
        col := next;
      };
      print_string("\n\n");
    };
    printf(
      "Displayed: %d%% (%d/%d)\n",
      count_displayed * 100 / t.count,
      count_displayed,
      t.count,
    );
    printf("Min      : %{Time_ns.Span}\n"^, t.min);
    printf("Max      : %{Time_ns.Span}\n"^, t.max);
    printf(
      "Mean     : %{Time_ns.Span}\n"^,
      Time_ns.Span.(/)(t.total, float(t.count)),
    );
  };
};

module Protocol = {
  [@deriving bin_io]
  type query = string;
  [@deriving bin_io]
  type response = string;

  let rpc =
    Rpc.create(
      ~name="test-rpc-latency",
      ~version=1,
      ~bin_query,
      ~bin_response,
    );

  module Quit = {
    [@deriving bin_io]
    type query = unit;
    [@deriving bin_io]
    type response = unit;

    let rpc =
      Rpc.create(
        ~name="test-rpc-latency-quit",
        ~version=1,
        ~bin_query,
        ~bin_response,
      );
  };
};

module Client = {
  let one_call = (~connection, ~stats, ~record, ~start, ~payload) =>
    Rpc.dispatch_exn(Protocol.rpc, connection, payload)
    >>> (
      _ => {
        let stop = Time_ns.now();
        let span = Time_ns.diff(stop, start);
        if (record^) {
          Stats.add(stats, ~span);
        };
      }
    );

  let rec loop = (~connection, ~stats, ~record, ~span_between_call, ~payload) => {
    let start = Time_ns.now();
    one_call(~connection, ~stats, ~record, ~start, ~payload);
    let now = Time_ns.now();
    let stop = Time_ns.add(start, span_between_call);
    let span = Time_ns.diff(stop, now);
    (
      if (Time_ns.Span.(<=)(span, Time_ns.Span.zero)) {
        Scheduler.yield();
      } else {
        Clock_ns.after(span);
      }
    )
    >>> (
      () => loop(~connection, ~stats, ~record, ~span_between_call, ~payload)
    );
  };

  let main = (msgs_per_sec, msg_size, ~host, ~port, ~rpc_impl, ()) => {
    let payload = String.make(msg_size, '\000');
    let%bind connection =
      Rpc_impl.make_client(rpc_impl, host, port) >>| Result.ok_exn;
    let stats = Stats.create();
    let record = ref(false);
    loop(
      ~connection,
      ~stats,
      ~record,
      ~span_between_call=Time_ns.Span.of_int_ns(1_000_000_000 / msgs_per_sec),
      ~payload,
    );
    let%bind () = Clock.after(sec(1.));
    record := true;
    let%map () = Clock.after(sec(5.));
    Stats.print(stats);
  };
};

module Client_long = {
  let pending = ref(0);
  let running = ref(false);
  let record = ref(false);

  let one_call = (~connection, ~stats, ~start, ~payload) => {
    incr(pending);
    Rpc.dispatch_exn(Protocol.rpc, connection, payload)
    >>> (
      _ => {
        decr(pending);
        let stop = Time_ns.now();
        let span = Time_ns.diff(stop, start);
        let span = float(Time_ns.Span.to_int_ns(span) / 1000);
        if (record^) {
          Rstats.update_in_place(stats, span);
        };
      }
    );
  };

  let rec loop = (~connection, ~stats, ~record, ~span_between_call, ~payload) =>
    if (running^) {
      let start = Time_ns.now();
      one_call(~connection, ~stats, ~start, ~payload);
      let now = Time_ns.now();
      let stop = Time_ns.add(start, span_between_call);
      let span = Time_ns.diff(stop, now);
      (
        if (Time_ns.Span.(<=)(span, Time_ns.Span.zero)) {
          Scheduler.yield();
        } else {
          Clock_ns.after(span);
        }
      )
      >>> (
        () => loop(~connection, ~stats, ~record, ~span_between_call, ~payload)
      );
    };

  let rec wait_for_pending = () =>
    if (pending^ > 0) {
      Clock.after(sec(0.01)) >>= wait_for_pending;
    } else {
      Deferred.unit;
    };

  let try_one = (csv_oc, ~connection, ~msgs_per_sec, ~payload) => {
    let stats = Rstats.create();
    record := false;
    running := true;
    assert(pending^ == 0);
    loop(
      ~connection,
      ~stats,
      ~record,
      ~span_between_call=Time_ns.Span.of_int_ns(1_000_000_000 / msgs_per_sec),
      ~payload,
    );
    let%bind () = Clock.after(sec(5.));
    record := true;
    let%bind () = Clock.after(sec(10.));
    running := false;
    record := false;
    let%map () = wait_for_pending();
    let real_msgs_per_sec = float(Rstats.samples(stats)) /. 10.;
    Core.printf(
      {|msgs/s : %f
mean   : %f us
stdev  : %f us
min    : %f us
max    : %f us

%!|},
      real_msgs_per_sec,
      Rstats.mean(stats),
      Rstats.stdev(stats),
      Rstats.min(stats),
      Rstats.max(stats),
    );
    Core.fprintf(
      csv_oc,
      "%f,%f,%f\n",
      real_msgs_per_sec,
      Rstats.mean(stats),
      Rstats.stdev(stats),
    );
  };

  let main = (csv_prefix, msg_size, ~host, ~port, ~rpc_impl, ()) => {
    let payload = String.make(msg_size, '\000');
    let%bind connection =
      Rpc_impl.make_client(rpc_impl, host, port) >>| Result.ok_exn;
    let csv_oc =
      ksprintf(
        Core.Out_channel.create,
        "out-%s-%d.csv",
        csv_prefix,
        msg_size,
      );
    Core.fprintf(csv_oc, "msgs/s,mean,stdev\n");
    let%map () =
      Deferred.List.iter(
        [
          100,
          200,
          300,
          400,
          500,
          600,
          700,
          800,
          900,
          1000,
          10_000,
          20_000,
          30_000,
          40_000,
          50_000,
          60_000,
          70_000,
          80_000,
          90_000,
          100_000,
          110_000,
          120_000,
          130_000,
          140_000,
          150_000,
          200_000,
          300_000,
          400_000,
          500_000,
          600_000,
          700_000,
          800_000,
          900_000,
          1_000_000,
          1_500_000,
          2_000_000,
        ],
        ~f=msgs_per_sec =>
        try_one(csv_oc, ~connection, ~msgs_per_sec, ~payload)
      );

    Core.Out_channel.close(csv_oc);
  };
};

module Server = {
  let implementations = [
    Rpc.implement'(Protocol.rpc, ((), s) => s),
    Rpc.implement'(Protocol.Quit.rpc, ((), ()) =>
      Clock.after(sec(1.0)) >>> (() => shutdown(0))
    ),
  ];

  let main = (~port, ~rpc_impl, ()) => {
    let implementations =
      Implementations.create_exn(~implementations, ~on_unknown_rpc=`Raise);

    let%bind _server =
      Rpc_impl.make_server(
        ~initial_connection_state=_ => (),
        ~implementations,
        ~port,
        rpc_impl,
      );

    Deferred.never();
  };
};

module Quit = {
  let main = (~host, ~port, ~rpc_impl, ()) => {
    let%bind connection =
      Rpc_impl.make_client(rpc_impl, host, port) >>| Result.ok_exn;
    let%map () = Rpc.dispatch_exn(Protocol.Quit.rpc, connection, ());
    shutdown(0);
  };
};

let server_command =
  Command.async_spec(
    ~summary="test server",
    Command.Spec.((empty ++ Spec.port()) ++ Rpc_impl.spec()),
    Server.main,
  );

let client_command =
  Command.async_spec(
    ~summary="test client",
    Command.Spec.(
      (
        (
          empty
          +> flag("msg-per-sec", required(int), ~doc="")
          +> flag(
               "msg-size",
               optional_with_default(0, int),
               ~doc=" message size",
             )
          ++ Spec.host()
        )
        ++ Spec.port()
      )
      ++ Rpc_impl.spec()
    ),
    Client.main,
  );

let client_long_command =
  Command.async_spec(
    ~summary="long test client",
    Command.Spec.(
      (
        (
          empty
          +> flag("csv-prefix", required(string), ~doc="")
          +> flag(
               "msg-size",
               optional_with_default(0, int),
               ~doc=" message size",
             )
          ++ Spec.host()
        )
        ++ Spec.port()
      )
      ++ Rpc_impl.spec()
    ),
    Client_long.main,
  );

let quit_command =
  Command.async_spec(
    ~summary="test quit",
    Command.Spec.(((empty ++ Spec.host()) ++ Spec.port()) ++ Rpc_impl.spec()),
    Quit.main,
  );

let () =
  Command_unix.run(
    Command.group(
      ~summary="Async RPC latency test",
      [
        ("server", server_command),
        ("client", client_command),
        ("client-long", client_long_command),
        ("quit-server", quit_command),
      ],
    ),
  );
