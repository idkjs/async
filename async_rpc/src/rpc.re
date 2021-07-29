open Core;
open Import;
module Transport = Rpc_transport;
module Low_latency_transport = Rpc_transport_low_latency;
module Any = Rpc_kernel.Any;
module Description = Rpc_kernel.Description;
module Implementation = Rpc_kernel.Implementation;
module Implementations = Rpc_kernel.Implementations;
module One_way = Rpc_kernel.One_way;
module Pipe_rpc = Rpc_kernel.Pipe_rpc;
module Rpc = Rpc_kernel.Rpc;
module State_rpc = Rpc_kernel.State_rpc;
module Pipe_close_reason = Rpc_kernel.Pipe_close_reason;

module Connection = {
  include Rpc_kernel.Connection;

  let create =
      (
        ~implementations=?,
        ~connection_state,
        ~max_message_size=?,
        ~handshake_timeout=?,
        ~heartbeat_config=?,
        ~description=?,
        reader,
        writer,
      ) =>
    create(
      ~implementations?,
      ~connection_state,
      ~handshake_timeout=?
        Option.map(
          handshake_timeout,
          ~f=Time_ns.Span.of_span_float_round_nearest,
        ),
      ~heartbeat_config?,
      ~description?,
      Transport.of_reader_writer(reader, writer, ~max_message_size?),
    );

  let contains_magic_prefix = reader =>
    switch%map (
      Deferred.Or_error.try_with(~run=`Schedule, ~rest=`Log, () =>
        Reader.peek_bin_prot(reader, contains_magic_prefix)
      )
    ) {
    | Error(_)
    | Ok(`Eof) => false
    | Ok(`Ok(b)) => b
    };

  let with_close =
      (
        ~implementations=?,
        ~max_message_size=?,
        ~handshake_timeout=?,
        ~heartbeat_config=?,
        ~description=?,
        ~connection_state,
        reader,
        writer,
        ~dispatch_queries,
        ~on_handshake_error,
      ) =>
    with_close(
      ~implementations?,
      ~handshake_timeout=?
        Option.map(
          handshake_timeout,
          ~f=Time_ns.Span.of_span_float_round_nearest,
        ),
      ~heartbeat_config?,
      ~description?,
      ~connection_state,
      Transport.of_reader_writer(reader, writer, ~max_message_size?),
      ~dispatch_queries,
      ~on_handshake_error,
    );

  let server_with_close =
      (
        ~max_message_size=?,
        ~handshake_timeout=?,
        ~heartbeat_config=?,
        ~description=?,
        reader,
        writer,
        ~implementations,
        ~connection_state,
        ~on_handshake_error,
      ) =>
    server_with_close(
      ~handshake_timeout=?
        Option.map(
          handshake_timeout,
          ~f=Time_ns.Span.of_span_float_round_nearest,
        ),
      ~heartbeat_config?,
      ~description?,
      Transport.of_reader_writer(reader, writer, ~max_message_size?),
      ~implementations,
      ~connection_state,
      ~on_handshake_error,
    );

  let collect_errors = (transport: Transport.t, ~f) => {
    let monitor = Transport.Writer.monitor(transport.writer);
    /* don't propagate errors up, we handle them here */
    ignore(Monitor.detach_and_get_error_stream(monitor));
    choose([
      choice(Monitor.get_next_error(monitor), e => Error(e)),
      choice(
        Monitor.try_with(
          ~run=`Schedule,
          ~rest=`Log,
          ~name="Rpc.Connection.collect_errors",
          f,
        ),
        Fn.id,
      ),
    ]);
  };

  type transport_maker = (Fd.t, ~max_message_size: int) => Transport.t;

  type on_handshake_error = [ | `Raise | `Ignore | `Call(Exn.t => unit)];

  let serve_with_transport =
      (
        ~handshake_timeout,
        ~heartbeat_config,
        ~implementations,
        ~description,
        ~connection_state,
        ~on_handshake_error,
        transport,
      ) => {
    let%bind res =
      collect_errors(transport, ~f=() =>
        switch%bind (
          Rpc_kernel.Connection.create(
            ~handshake_timeout=?
              Option.map(
                handshake_timeout,
                ~f=Time_ns.Span.of_span_float_round_nearest,
              ),
            ~heartbeat_config?,
            ~implementations,
            ~description,
            ~connection_state,
            transport,
          )
        ) {
        | Ok(t) => close_finished(t)
        | Error(handshake_error) =>
          switch (on_handshake_error) {
          | `Call(f) => f(handshake_error)
          | `Raise => raise(handshake_error)
          | `Ignore => ()
          };
          Deferred.unit;
        }
      );

    let%map () = Transport.close(transport);
    Result.ok_exn(res);
  };

  let make_serve_func =
      (
        serve_with_transport_handler,
        ~implementations,
        ~initial_connection_state,
        ~where_to_listen,
        ~max_connections=?,
        ~backlog=?,
        ~drop_incoming_connections=?,
        ~time_source=?,
        ~max_message_size=?,
        ~make_transport=?,
        ~handshake_timeout=?,
        ~heartbeat_config=?,
        ~auth=?,
        ~on_handshake_error=`Ignore,
        ~on_handler_error=?,
        (),
      ) =>
    serve_with_transport_handler(
      ~where_to_listen,
      ~max_connections?,
      ~backlog?,
      ~drop_incoming_connections?,
      ~time_source?,
      ~max_message_size?,
      ~make_transport?,
      ~auth?,
      ~on_handler_error?,
      (~client_addr, ~server_addr, transport) => {
        let description = {
          let server_addr = (server_addr :> Socket.Address.t);
          let client_addr = (client_addr :> Socket.Address.t);
          Info.create_s(
            [%message
              "TCP server"(
                server_addr: Socket.Address.t,
                client_addr: Socket.Address.t,
              )
            ],
          );
        };

        serve_with_transport(
          ~handshake_timeout,
          ~heartbeat_config,
          ~implementations,
          ~description,
          ~connection_state=
            conn => initial_connection_state(client_addr, conn),
          ~on_handshake_error,
          transport,
        );
      },
    );

  /* eta-expand [implementations] to avoid value restriction. */
  let serve = (~implementations) =>
    make_serve_func(Rpc_transport.Tcp.serve, ~implementations);

  /* eta-expand [implementations] to avoid value restriction. */
  let serve_inet = (~implementations) =>
    make_serve_func(Rpc_transport.Tcp.serve_inet, ~implementations);

  let default_handshake_timeout_float =
    Time_ns.Span.to_span_float_round_nearest(
      Async_rpc_kernel.Async_rpc_kernel_private.default_handshake_timeout,
    );

  let client' =
      (
        ~implementations=?,
        ~max_message_size=?,
        ~make_transport=?,
        ~handshake_timeout as handshake_timeout_float=default_handshake_timeout_float,
        ~heartbeat_config=?,
        ~description=?,
        where_to_connect,
      ) => {
    let handshake_timeout =
      Time_ns.Span.of_span_float_round_nearest(handshake_timeout_float);

    let finish_handshake_by = Time_ns.add(Time_ns.now(), handshake_timeout);
    switch%bind (
      Rpc_transport.Tcp.connect(
        ~max_message_size?,
        ~make_transport?,
        ~tcp_connect_timeout=handshake_timeout,
        where_to_connect,
      )
    ) {
    | Error(_) as error => return(error)
    | [@implicit_arity] Ok(transport, sock_peername) =>
      let description =
        switch (description) {
        | None =>
          Info.create(
            "Client connected via TCP",
            where_to_connect,
            [%sexp_of: Tcp.Where_to_connect.t(_)],
          )
        | Some(desc) =>
          Info.tag_arg(
            desc,
            "via TCP",
            where_to_connect,
            [%sexp_of: Tcp.Where_to_connect.t(_)],
          )
        };

      let handshake_timeout =
        Time_ns.diff(finish_handshake_by, Time_ns.now());
      let%bind rpc_connection =
        switch (implementations) {
        | None =>
          let {Client_implementations.connection_state, implementations} =
            Client_implementations.null();

          Rpc_kernel.Connection.create(
            transport,
            ~handshake_timeout,
            ~heartbeat_config?,
            ~implementations,
            ~description,
            ~connection_state,
          );
        | Some({Client_implementations.connection_state, implementations}) =>
          Rpc_kernel.Connection.create(
            transport,
            ~handshake_timeout,
            ~heartbeat_config?,
            ~implementations,
            ~description,
            ~connection_state,
          )
        };

      switch (rpc_connection) {
      | Ok(t) => return([@implicit_arity] Ok(sock_peername, t))
      | Error(_) as error =>
        let%bind () = Transport.close(transport);
        return(error);
      };
    };
  };

  let client =
      (
        ~implementations=?,
        ~max_message_size=?,
        ~make_transport=?,
        ~handshake_timeout=?,
        ~heartbeat_config=?,
        ~description=?,
        where_to_connect,
      ) =>
    client'(
      ~implementations?,
      ~max_message_size?,
      ~make_transport?,
      ~handshake_timeout?,
      ~heartbeat_config?,
      ~description?,
      where_to_connect,
    )
    >>|? snd;

  let with_client' =
      (
        ~implementations=?,
        ~max_message_size=?,
        ~make_transport=?,
        ~handshake_timeout=?,
        ~heartbeat_config=?,
        ~description=?,
        where_to_connect,
        f,
      ) =>
    client'(
      ~implementations?,
      ~max_message_size?,
      ~make_transport?,
      ~handshake_timeout?,
      ~heartbeat_config?,
      ~description?,
      where_to_connect,
    )
    >>=? (
      ((remote_server, t)) => {
        let%bind result =
          Monitor.try_with(~run=`Schedule, ~rest=`Log, () =>
            f(~remote_server, t)
          );

        let%map () =
          close(
            t,
            ~reason=Info.of_string("Rpc.Connection.with_client finished"),
          );
        result;
      }
    );

  let with_client =
      (
        ~implementations=?,
        ~max_message_size=?,
        ~make_transport=?,
        ~handshake_timeout=?,
        ~heartbeat_config=?,
        where_to_connect,
        f,
      ) =>
    with_client'(
      ~implementations?,
      ~max_message_size?,
      ~make_transport?,
      ~handshake_timeout?,
      ~heartbeat_config?,
      where_to_connect,
      (~remote_server as _) =>
      f
    );
};
