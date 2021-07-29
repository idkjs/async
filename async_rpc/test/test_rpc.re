open! Core;
open Poly;
open! Async;
open! Import;
module Debug = Async_kernel_private.Debug;

let () = Backtrace.elide := true;
let max_message_size = 1_000_000;

let test = (~make_transport, ~imp1, ~imp2, ~state1, ~state2, ~f, ()) => {
  let%bind (`Reader(r1), `Writer(w2)) =
    Unix.pipe(Info.of_string("rpc_test 1"));
  let%bind (`Reader(r2), `Writer(w1)) =
    Unix.pipe(Info.of_string("rpc_test 2"));
  let t1 = make_transport((r1, w1));
  let t2 = make_transport((r2, w2));
  let s = imp =>
    if (List.length(imp) > 0) {
      Some(
        Rpc.Implementations.create_exn(
          ~implementations=imp,
          ~on_unknown_rpc=`Close_connection,
        ),
      );
    } else {
      None;
    };

  let s1 = s(imp1);
  let s2 = s(imp2);
  let conn1_ivar = Ivar.create();
  let f2_done =
    Async_rpc_kernel.Rpc.Connection.with_close(
      ~implementations=?s2,
      t2,
      ~dispatch_queries=
        conn2 => {
          let%bind conn1 = Ivar.read(conn1_ivar);
          f(conn1, conn2);
        },
      ~connection_state=_ => state2,
      ~on_handshake_error=`Raise,
    );

  Async_rpc_kernel.Rpc.Connection.with_close(
    ~implementations=?s1,
    t1,
    ~dispatch_queries=
      conn1 => {
        Ivar.fill(conn1_ivar, conn1);
        f2_done;
      },
    ~connection_state=_ => state1,
    ~on_handshake_error=`Raise,
  );
};

let test1 = (~make_transport, ~imp, ~state, ~f) =>
  test(~make_transport, ~imp1=imp, ~state1=state, ~imp2=[], ~state2=(), ~f);

module Pipe_count_error = {
  [@deriving bin_io]
  type t = [ | `Argument_must_be_positive];
};

let pipe_count_rpc =
  Rpc.Pipe_rpc.create(
    ~name="pipe_count",
    ~version=0,
    ~bin_query=Int.bin_t,
    ~bin_response=Int.bin_t,
    ~bin_error=Pipe_count_error.bin_t,
    (),
  );

let pipe_wait_rpc =
  Rpc.Pipe_rpc.create(
    ~name="pipe_wait",
    ~version=0,
    ~bin_query=Unit.bin_t,
    ~bin_response=Unit.bin_t,
    ~bin_error=Unit.bin_t,
    (),
  );

let pipe_count_imp =
  Rpc.Pipe_rpc.implement(pipe_count_rpc, ((), n) =>
    if (n < 0) {
      return(Error(`Argument_must_be_positive));
    } else {
      let (pipe_r, pipe_w) = Pipe.create();
      upon(
        Deferred.List.iter(List.init(n, ~f=Fn.id), ~how=`Sequential, ~f=i =>
          Pipe.write(pipe_w, i)
        ),
        () =>
        Pipe.close(pipe_w)
      );
      return(Ok(pipe_r));
    }
  );

let pipe_wait_imp = ivar =>
  Rpc.Pipe_rpc.implement(
    pipe_wait_rpc,
    ((), ()) => {
      let (pipe_r, pipe_w) = Pipe.create();
      Pipe.write(pipe_w, ())
      >>> (
        () =>
          Ivar.read(ivar)
          >>> (() => Pipe.write(pipe_w, ()) >>> (() => Pipe.close(pipe_w)))
      );
      return(Ok(pipe_r));
    },
  );

let make_tests = (~make_transport, ~transport_name) =>
  List.mapi(
    ~f=(i, f) => (sprintf("rpc-%s-%d", transport_name, i), f),
    [
      test1(
        ~make_transport,
        ~imp=[pipe_count_imp],
        ~state=(),
        ~f=(_, conn) => {
          let n = 3;
          let%bind (pipe_r, _id) =
            Rpc.Pipe_rpc.dispatch_exn(pipe_count_rpc, conn, n);
          let%bind x =
            Pipe.fold_without_pushback(
              pipe_r,
              ~init=0,
              ~f=(x, i) => {
                assert(x == i);
                i + 1;
              },
            );

          [%test_result: int](~expect=n, x);
          Deferred.unit;
        },
      ),
      test1(
        ~make_transport,
        ~imp=[pipe_count_imp],
        ~state=(),
        ~f=(_, conn) => {
          let%bind result = Rpc.Pipe_rpc.dispatch(pipe_count_rpc, conn, -1);
          switch (result) {
          | Ok(Ok(_))
          | Error(_) => assert(false)
          | Ok(Error(`Argument_must_be_positive)) => Deferred.unit
          };
        },
      ),
      {
        let ivar = Ivar.create();
        test1(
          ~make_transport,
          ~imp=[pipe_wait_imp(ivar)],
          ~state=(),
          ~f=(conn1, conn2) => {
            /* Test that the pipe is flushed when the connection is closed. */
            let%bind (pipe_r, _id) =
              Rpc.Pipe_rpc.dispatch_exn(pipe_wait_rpc, conn2, ());
            let%bind res = Pipe.read(pipe_r);
            assert(res == `Ok());
            don't_wait_for(Rpc.Connection.close(conn1));
            Ivar.fill(ivar, ());
            let%bind res = Pipe.read(pipe_r);
            assert(res == `Ok());
            Deferred.unit;
          },
        );
      },
    ],
  );

let%expect_test _ = {
  let make_transport_std = ((fd_r, fd_w)): Rpc.Transport.t => {
    reader:
      Reader.create(fd_r)
      |> Rpc.Transport.Reader.of_reader(~max_message_size),
    writer:
      Writer.create(fd_w)
      |> Rpc.Transport.Writer.of_writer(~max_message_size),
  };

  let make_transport_low_latency = ((fd_r, fd_w)): Rpc.Transport.t => {
    reader: Rpc.Low_latency_transport.Reader.create(fd_r, ~max_message_size),
    writer: Rpc.Low_latency_transport.Writer.create(fd_w, ~max_message_size),
  };

  let%bind () =
    Deferred.List.iter(
      ~f=
        ((name, f)) => {
          print_s([%message name]);
          f();
        },
      make_tests(~make_transport=make_transport_std, ~transport_name="std")
      @ make_tests(
          ~make_transport=make_transport_low_latency,
          ~transport_name="low-latency",
        ),
    );

  %expect
  {|
    rpc-std-0
    rpc-std-1
    rpc-std-2
    rpc-low-latency-0
    rpc-low-latency-1
    rpc-low-latency-2 |};
  return();
};

let%expect_test "[Connection.create] shouldn't raise" = {
  let%bind (`Reader(r1), `Writer(w1)) =
    Unix.pipe(Info.of_string("rpc_test 1"));
  let%bind (`Reader(_), `Writer(w2)) =
    Unix.pipe(Info.of_string("rpc_test 2"));
  let result =
    Deferred.create(ivar =>
      Monitor.try_with(~run=`Schedule, ~rest=`Log, () =>
        Rpc.Connection.create(
          ~connection_state=Fn.const(),
          Reader.create(r1),
          Writer.create(w2),
        )
      )
      >>> (
        fun
        | Error(exn) => Ivar.fill(ivar, `Raised(exn))
        | Ok(Ok(_: Rpc.Connection.t)) => assert(false)
        | Ok(Error(exn)) => Ivar.fill(ivar, `Returned(exn))
      )
    );

  let writer1 = Writer.create(w1);
  /* We must write at least [Header.length] (8) bytes. */
  Writer.write(writer1, "failfail");
  let%bind () = Writer.flushed(writer1);
  let%bind result = result;
  print_s(
    [%message ""(~_=result: [ | `Raised(Exn.t) | `Returned(Exn.t)])],
  );
  %expect
  {|
    (Returned
     (connection.ml.Handshake_error.Handshake_error
      ((Reading_header_failed
        (monitor.ml.Error
         (Failure "unsafe_read_int64: value cannot be represented unboxed!")
         ("<backtrace elided in test>")))
       <created-directly>))) |};
  return();
};

open! Rpc;

let%test_unit "Open dispatches see connection closed error" =
  Thread_safe.block_on_async_exn(() =>
    let bin_t = Bin_prot.Type_class.bin_unit;
    let rpc =
      Rpc.create(
        ~version=1,
        ~name="__TEST_Async_rpc.Rpc",
        ~bin_query=bin_t,
        ~bin_response=bin_t,
      );

    let serve = () => {
      let implementation = Rpc.implement(rpc, ((), ()) => Deferred.never());
      let implementations =
        Implementations.create_exn(
          ~implementations=[implementation],
          ~on_unknown_rpc=`Raise,
        );

      Connection.serve(
        ~initial_connection_state=(_, _) => (),
        ~implementations,
        ~where_to_listen=Tcp.Where_to_listen.of_port_chosen_by_os,
        (),
      );
    };

    let client = (~port) => {
      let%bind connection =
        Connection.client(
          Tcp.Where_to_connect.of_host_and_port({host: "localhost", port}),
        )
        >>| Result.ok_exn;

      let res = Rpc.dispatch(rpc, connection, ());
      don't_wait_for(Connection.close(connection));
      switch%map (res) {
      | Ok () => failwith("Dispatch should have failed")
      | Error(err) =>
        [%test_eq: string](
          sprintf(
            "((rpc_error (Connection_closed (Rpc.Connection.close)))\n (connection_description (\"Client connected via TCP\" (localhost %d)))\n (rpc_tag __TEST_Async_rpc.Rpc) (rpc_version 1))",
            port,
          ),
          Error.to_string_hum(err),
        )
      };
    };

    let%bind server = serve();
    let port = Tcp.Server.listening_on(server);
    let%bind () = client(~port);
    Tcp.Server.close(server);
  );
