open Core;
open Import;
module Async_reader = Reader;
module Async_writer = Writer;
module Kernel_transport = Rpc_kernel.Transport;
module Header = Kernel_transport.Header;
module Handler_result = Kernel_transport.Handler_result;
module Send_result = Kernel_transport.Send_result;

module With_limit: {
  [@deriving sexp_of]
  type t('a) =
    pri {
      t: 'a,
      max_message_size: int,
    };

  let create: ('a, ~max_message_size: int) => t('a);
  let message_size_ok: (t(_), ~payload_len: int) => bool;
  let check_message_size: (t(_), ~payload_len: int) => unit;
} = {
  [@deriving sexp_of]
  type t('a) = {
    t: 'a,
    max_message_size: int,
  };

  let create = (t, ~max_message_size) => {
    if (max_message_size < 0) {
      failwithf(
        "Rpc_transport.With_limit.create got negative max message size: %d",
        max_message_size,
        (),
      );
    };
    {t, max_message_size};
  };

  let message_size_ok = (t, ~payload_len) =>
    payload_len >= 0 && payload_len <= t.max_message_size;

  let check_message_size = (t, ~payload_len) =>
    if (!message_size_ok(t, ~payload_len)) {
      failwiths(
        ~here=[%here],
        "Rpc_transport: message too small or too big",
        (`Message_size(payload_len), `Max_message_size(t.max_message_size)),
        [%sexp_of: ([ | `Message_size(int)], [ | `Max_message_size(int)])],
      );
    };
};

module Unix_reader = {
  open With_limit;

  [@deriving sexp_of]
  type t = With_limit.t(Reader.t);

  let create = (~reader, ~max_message_size) =>
    With_limit.create(reader, ~max_message_size);
  let close = t => Reader.close(t.t);
  let is_closed = t => Reader.is_closed(t.t);

  let all_unit_then_return = (l, ret_val) =>
    switch (l) {
    | [] => return(ret_val) /* avoid deferred operations in the common case */
    | _ =>
      let%map () = Deferred.all_unit(l);
      ret_val;
    };

  let read_forever = (t, ~on_message, ~on_end_of_batch) => {
    let finish_loop = (~consumed, ~need, ~wait_before_reading) => {
      on_end_of_batch();
      all_unit_then_return(
        wait_before_reading,
        `Consumed((consumed, `Need(need))),
      );
    };

    let rec loop = (buf, ~pos, ~len, ~consumed, ~wait_before_reading) =>
      if (len < Header.length) {
        finish_loop(~consumed, ~need=Header.length, ~wait_before_reading);
      } else {
        let payload_len = Header.unsafe_get_payload_length(buf, ~pos);
        let total_len = Header.length + payload_len;
        With_limit.check_message_size(t, ~payload_len);
        if (len < total_len) {
          finish_loop(~consumed, ~need=total_len, ~wait_before_reading);
        } else {
          let consumed = consumed + total_len;
          let result: Handler_result.t(_) = (
            on_message(buf, ~pos=pos + Header.length, ~len=payload_len):
              Handler_result.t(_)
          );

          switch (result) {
          | Stop(x) =>
            all_unit_then_return(
              wait_before_reading,
              `Stop_consumed((x, consumed)),
            )
          | Continue =>
            loop(
              buf,
              ~pos=pos + total_len,
              ~len=len - total_len,
              ~consumed,
              ~wait_before_reading,
            )
          | Wait(d) =>
            let wait_before_reading =
              if (Deferred.is_determined(d)) {
                wait_before_reading;
              } else {
                [d, ...wait_before_reading];
              };

            loop(
              buf,
              ~pos=pos + total_len,
              ~len=len - total_len,
              ~consumed,
              ~wait_before_reading,
            );
          };
        };
      };

    let handle_chunk = (buf, ~pos, ~len) =>
      loop(buf, ~pos, ~len, ~consumed=0, ~wait_before_reading=[]);

    switch%map (Reader.read_one_chunk_at_a_time(t.t, ~handle_chunk)) {
    | `Eof
    | `Eof_with_unconsumed_data(_) => Error(`Eof)
    | `Stopped(x) => Ok(x)
    };
  };
};

module Unix_writer = {
  open With_limit;

  [@deriving sexp_of]
  type t = With_limit.t(Writer.t);

  let create = (~writer, ~max_message_size) => {
    /* Prevent exceptions in the writer when the other side disconnects. Note that "stale
       data in buffer" exceptions are not an issue when the consumer leaves, since
       [Rpc_kernel.Connection] takes care of closing the transport when the consumer
       leaves. */
    Writer.set_raise_when_consumer_leaves(writer, false);
    With_limit.create(writer, ~max_message_size);
  };

  let close = t => Writer.close(t.t);
  let is_closed = t => Writer.is_closed(t.t);
  let monitor = t => Writer.monitor(t.t);
  let bytes_to_write = t => Writer.bytes_to_write(t.t);
  let stopped = t =>
    Deferred.any([Writer.close_started(t.t), Writer.consumer_left(t.t)]);
  let flushed = t => Writer.flushed(t.t);
  let ready_to_write = flushed;

  let bin_write_payload_length = (buf, ~pos, x) => {
    Header.unsafe_set_payload_length(buf, ~pos, x);
    pos + Header.length;
  };

  let send_bin_prot_internal =
      (t, bin_writer: Bin_prot.Type_class.writer(_), x, ~followup_len)
      : Send_result.t(_) =>
    if (!Writer.is_closed(t.t)) {
      let data_len = bin_writer.size(x);
      let payload_len = data_len + followup_len;
      if (message_size_ok(t, ~payload_len)) {
        Writer.write_bin_prot_no_size_header(
          t.t,
          ~size=Header.length,
          bin_write_payload_length,
          payload_len,
        );
        Writer.write_bin_prot_no_size_header(
          t.t,
          ~size=data_len,
          bin_writer.write,
          x,
        );
        Sent();
      } else {
        Message_too_big({
          size: payload_len,
          max_message_size: t.max_message_size,
        });
      };
    } else {
      Closed;
    };

  let send_bin_prot = (t, bin_writer, x) =>
    send_bin_prot_internal(t, bin_writer, x, ~followup_len=0);

  let send_bin_prot_and_bigstring =
      (t, bin_writer, x, ~buf, ~pos, ~len): Send_result.t(_) =>
    switch (send_bin_prot_internal(t, bin_writer, x, ~followup_len=len)) {
    | Sent () =>
      Writer.write_bigstring(t.t, buf, ~pos, ~len);
      Sent();
    | error => error
    };

  let send_bin_prot_and_bigstring_non_copying =
      (t, bin_writer, x, ~buf, ~pos, ~len): Send_result.t(_) =>
    switch (send_bin_prot_internal(t, bin_writer, x, ~followup_len=len)) {
    | Sent () =>
      Writer.schedule_bigstring(t.t, buf, ~pos, ~len);
      Sent(Writer.flushed(t.t));
    | (Closed | Message_too_big(_)) as r => r
    };
};

/* unfortunately, copied from reader0.ml */
let default_max_message_size = 100 * 1024 * 1024;

module Reader = {
  include Kernel_transport.Reader;

  let of_reader = (~max_message_size=default_max_message_size, reader) =>
    pack(
      (module Unix_reader),
      Unix_reader.create(~reader, ~max_message_size),
    );
};

module Writer = {
  include Kernel_transport.Writer;

  let of_writer = (~max_message_size=default_max_message_size, writer) =>
    pack(
      (module Unix_writer),
      Unix_writer.create(~writer, ~max_message_size),
    );
};

[@deriving sexp_of]
type t =
  Kernel_transport.t = {
    reader: Reader.t,
    writer: Writer.t,
  };

let close = Kernel_transport.close;

let of_reader_writer = (~max_message_size=?, reader, writer) => {
  reader: Reader.of_reader(~max_message_size?, reader),
  writer: Writer.of_writer(~max_message_size?, writer),
};

let of_fd =
    (~buffer_age_limit=?, ~reader_buffer_size=?, ~max_message_size, fd) =>
  of_reader_writer(
    ~max_message_size,
    Async_unix.Reader.create(~buf_len=?reader_buffer_size, fd),
    Async_unix.Writer.create(~buffer_age_limit?, fd),
  );

module Tcp = {
  let default_transport_maker = (fd, ~max_message_size) =>
    of_fd(fd, ~max_message_size);

  let make_serve_func =
      (
        tcp_creator,
        ~where_to_listen,
        ~max_connections=?,
        ~backlog=?,
        ~drop_incoming_connections=?,
        ~time_source=?,
        ~max_message_size=default_max_message_size,
        ~make_transport=default_transport_maker,
        ~auth=_ => true,
        ~on_handler_error=`Ignore,
        handle_transport,
      ) =>
    tcp_creator(
      ~max_connections?,
      ~max_accepts_per_batch=?None,
      ~backlog?,
      ~drop_incoming_connections?,
      ~socket=?None,
      ~time_source?,
      ~on_handler_error,
      where_to_listen,
      (client_addr, socket) =>
      switch (auth(client_addr)) {
      | false => return()
      | true =>
        let transport = make_transport(~max_message_size, Socket.fd(socket));
        let%bind result =
          Monitor.try_with(~run=`Schedule, ~rest=`Raise, () =>
            handle_transport(
              ~client_addr,
              ~server_addr=Socket.getsockname(socket),
              transport,
            )
          );

        let%bind () = close(transport);
        switch (result) {
        | Ok () => return()
        | Error(exn) => raise(exn)
        };
      }
    );

  /* eta-expand [where_to_listen] to avoid value restriction. */
  let serve = (~where_to_listen) =>
    make_serve_func(Tcp.Server.create_sock, ~where_to_listen);

  /* eta-expand [where_to_listen] to avoid value restriction. */
  let serve_inet = (~where_to_listen) =>
    make_serve_func(Tcp.Server.create_sock_inet, ~where_to_listen);

  let connect =
      (
        ~max_message_size=default_max_message_size,
        ~make_transport=default_transport_maker,
        ~tcp_connect_timeout=Async_rpc_kernel.Async_rpc_kernel_private.default_handshake_timeout,
        where_to_connect,
      ) => {
    let%bind sock =
      Monitor.try_with(~run=`Schedule, ~rest=`Log, () =>
        Tcp.connect_sock(
          ~timeout=
            Time_ns.Span.to_span_float_round_nearest(tcp_connect_timeout),
          where_to_connect,
        )
      );

    switch (sock) {
    | Error(_) as error => return(error)
    | Ok(sock) =>
      switch (Socket.getpeername(sock)) {
      | exception exn_could_be_raised_if_the_socket_is_diconnected_now =>
        Socket.shutdown(sock, `Both);
        don't_wait_for(Unix.close(Socket.fd(sock)));
        return(Error(exn_could_be_raised_if_the_socket_is_diconnected_now));
      | sock_peername =>
        let transport = make_transport(Socket.fd(sock), ~max_message_size);
        return([@implicit_arity] Ok(transport, sock_peername));
      }
    };
  };
};
