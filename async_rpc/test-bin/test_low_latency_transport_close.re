open! Core;
open! Async;
module Thread = Core_thread;

module Rpcs = {
  module From_server = {
    [@deriving (bin_io, sexp)]
    type t = string;

    let rpc =
      Rpc.One_way.create(~name="from-server", ~version=0, ~bin_msg=bin_t);
    let impl = Rpc.One_way.implement(rpc, (_, _) => Thread.delay(0.0001));
  };

  module From_client = {
    [@deriving (bin_io, sexp)]
    type t = string;

    let rpc =
      Rpc.One_way.create(~name="from-client", ~version=0, ~bin_msg=bin_t);
    let impl = Rpc.One_way.implement(rpc, (_, _) => Thread.delay(0.0001));
  };
};

let message = Bytes.create(200) |> Bytes.to_string;

let low_latency_config =
  Rpc.Low_latency_transport.Config.create(
    ~write_timeout=Time_ns.Span.of_sec(5.),
    (),
  );

let make_transport = (~use_regular_transport) =>
  if (use_regular_transport) {
    None;
  } else {
    Some(
      (fd, ~max_message_size) =>
        Rpc.Low_latency_transport.create(
          ~max_message_size,
          ~config=low_latency_config,
          fd,
        ),
    );
  };

let heartbeat_config =
  Rpc.Connection.Heartbeat_config.create(
    ~timeout=Time_ns.Span.of_sec(5.),
    ~send_every=Time_ns.Span.of_sec(1.),
    (),
  );

let send_a_lot_of_messages = (rpc, conn) =>
  Deferred.forever(
    (),
    () => {
      for (_i in 1 to 1_000) {
        ignore(Rpc.One_way.dispatch(rpc, conn, message));
      };
      Scheduler.yield();
    },
  );

let print_connection_status = (~side, conn) => {
  upon(Rpc.Connection.close_reason(~on_close=`started, conn), info =>
    Core.printf(
      "%s rpc connection close started because of %{sexp:Info.t}\n%!"^,
      side,
      info,
    )
  );
  upon(Rpc.Connection.close_reason(~on_close=`finished, conn), info =>
    Core.printf(
      "%s rpc connection close finished because of %{sexp:Info.t}\n%!"^,
      side,
      info,
    )
  );
  Clock.every(sec(1.), () =>
    Core.printf(
      "%s rpc connection is_closed = %b\n%!",
      side,
      Rpc.Connection.is_closed(conn),
    )
  );
};

let run_server = (~port, ~closing_mode, ~use_regular_transport) => {
  let client_conn = Ivar.create();
  let%bind server =
    Rpc.Connection.serve(
      ~implementations=
        Rpc.Implementations.create_exn(
          ~implementations=[Rpcs.From_client.impl],
          ~on_unknown_rpc=`Raise,
        ),
      ~heartbeat_config,
      ~initial_connection_state=
        (_, conn) => {
          Ivar.fill(client_conn, conn);
          upon(Deferred.unit, () =>
            send_a_lot_of_messages(Rpcs.From_server.rpc, conn)
          );
        },
      ~where_to_listen=Tcp.Where_to_listen.of_port(port),
      ~make_transport=?make_transport(~use_regular_transport),
      (),
    );

  let%bind client_conn = Ivar.read(client_conn);
  print_connection_status(~side="server", client_conn);
  let%bind () = after(sec(5.));
  switch (closing_mode) {
  | `Close_connection =>
    Core.printf("Running [Rpc.Connection.close]\n%!");
    don't_wait_for(Rpc.Connection.close(client_conn));
  | `Tcp_server_close =>
    Core.printf(
      "Running [Tcp.Server.close ~close_existing_connections:true\n%!",
    );
    don't_wait_for(
      Tcp.Server.close(~close_existing_connections=true, server),
    );
  | `Shutdown =>
    Core.printf("Running [Shutdown.shutdown]\n%!");
    Shutdown.shutdown(1);
  };
  Rpc.Connection.close_finished(client_conn);
};

let run_client = (~host, ~port, ~use_regular_transport) => {
  let%bind conn =
    Rpc.Connection.client(
      ~implementations={
        connection_state: _ => ref(0),
        implementations:
          Rpc.Implementations.create_exn(
            ~implementations=[Rpcs.From_server.impl],
            ~on_unknown_rpc=`Raise,
          ),
      },
      ~heartbeat_config,
      ~make_transport=?make_transport(~use_regular_transport),
      Tcp.Where_to_connect.of_host_and_port(
        Host_and_port.create(~host, ~port),
      ),
    )
    >>| Result.ok_exn;

  send_a_lot_of_messages(Rpcs.From_client.rpc, conn);
  print_connection_status(~side="client", conn);
  Rpc.Connection.close_finished(conn);
};

let valid_closing_modes = "(close-connection|tcp-server-close|shutdown)";

let server_command =
  Command.async(
    ~summary="",
    {
      open Command.Let_syntax;
      let%map_open port = flag("port", required(int), ~doc="")
      and closing_mode =
        flag(
          "closing-mode",
          required(string),
          ~doc=
            valid_closing_modes
            ++ " different ways of closing on the server side",
        )
      and use_regular_transport = flag("regular-transport", no_arg, ~doc="");
      () => {
        let closing_mode =
          switch (String.uppercase(closing_mode)) {
          | "CLOSE-CONNECTION" => `Close_connection
          | "TCP-SERVER-CLOSE" => `Tcp_server_close
          | "SHUTDOWN" => `Shutdown
          | _ =>
            raise_s(
              [%message
                "invalid closing mode selection"(
                  valid_closing_modes,
                  closing_mode: string,
                )
              ],
            )
          };

        run_server(~port, ~closing_mode, ~use_regular_transport);
      };
    },
  );

let client_command =
  Command.async(
    ~summary="",
    {
      open Command.Let_syntax;
      let%map_open host = flag("host", optional(string), ~doc="")
      and port = flag("port", required(int), ~doc="")
      and use_regular_transport = flag("regular-transport", no_arg, ~doc="");
      () => {
        let host = Option.value(host, ~default=Unix.gethostname());
        run_client(~host, ~port, ~use_regular_transport);
      };
    },
  );

let command =
  Command.group(
    ~summary="",
    [("server", server_command), ("client", client_command)],
  );

let () = Command_unix.run(command);
