open Core;
open Import;
module Kernel_transport = Rpc_kernel.Transport;
module Header = Kernel_transport.Header;
module Handler_result = Kernel_transport.Handler_result;
module Send_result = Kernel_transport.Send_result;

[@noalloc]
external writev2:
  (
    Core_unix.File_descr.t,
    ~buf1: Bigstring.t,
    ~pos1: int,
    ~len1: int,
    ~buf2: Bigstring.t,
    ~pos2: int,
    ~len2: int
  ) =>
  Unix.Syscall_result.Int.t =
  "async_extra_rpc_writev2_byte" "async_extra_rpc_writev2";

module Config = {
  /* Same as the default value of [buffer_age_limit] for [Async_unix.Writer] */
  let default_write_timeout = Time_ns.Span.of_min(2.);

  /* No maximum */
  let default_max_message_size = Int.max_value;
  let default_max_buffer_size = Int.max_value;

  /* In general we'll send 1 message per job, if we send 2 there is a good chance we are
     sending a batch.

     Default should actually be 1, but there was a bug that made it 2 in practice, so we
     keep 2 as a default. */
  let default_start_batching_after_num_messages = 2;

  /* Arbitrary choices. */
  let default_initial_buffer_size = 64 * 1024;
  let default_buffering_threshold_in_bytes = 32 * 1024;

  [@deriving sexp]
  type t = {
    [@default default_max_message_size]
    max_message_size: int,
    [@default default_initial_buffer_size]
    initial_buffer_size: int,
    [@default default_max_buffer_size]
    max_buffer_size: int,
    [@default default_write_timeout]
    write_timeout: Time_ns.Span.t,
    [@default default_buffering_threshold_in_bytes]
    buffering_threshold_in_bytes: int,
    [@default default_start_batching_after_num_messages]
    start_batching_after_num_messages: int,
  };

  let validate = t => {
    if (t.initial_buffer_size <= 0
        || t.max_message_size <= 0
        || t.initial_buffer_size > t.max_buffer_size
        || t.max_message_size > t.max_buffer_size
        || t.buffering_threshold_in_bytes < 0
        || t.start_batching_after_num_messages < 0
        || Time_ns.Span.(<=)(t.write_timeout, Time_ns.Span.zero)) {
      failwiths(
        ~here=[%here],
        "Rpc_transport_low_latency.Config.validate: invalid config",
        t,
        sexp_of_t,
      );
    };
    t;
  };

  let t_of_sexp = sexp => t_of_sexp(sexp) |> validate;

  let create =
      (
        ~max_message_size=default_max_message_size,
        ~initial_buffer_size=default_initial_buffer_size,
        ~max_buffer_size=default_max_buffer_size,
        ~write_timeout=default_write_timeout,
        ~buffering_threshold_in_bytes=default_buffering_threshold_in_bytes,
        ~start_batching_after_num_messages=default_start_batching_after_num_messages,
        (),
      ) =>
    validate({
      max_message_size,
      initial_buffer_size,
      max_buffer_size,
      write_timeout,
      buffering_threshold_in_bytes,
      start_batching_after_num_messages,
    });

  let default = create();

  let message_size_ok = (t, ~payload_len) =>
    payload_len >= 0 && payload_len <= t.max_message_size;

  let check_message_size = (t, ~payload_len) =>
    if (!message_size_ok(t, ~payload_len)) {
      raise_s(
        [%sexp
          (
            "Rpc_transport_low_latency: message too small or too big",
            {message_size: (payload_len: int), config: (t: t)},
          )
        ],
      );
    };

  let grow_buffer = (t, buf, ~new_size_request) => {
    assert(new_size_request > Bigstring.length(buf));
    if (new_size_request > t.max_buffer_size) {
      raise_s(
        [%sexp
          (
            "Rpc_transport_low_latency: cannot grow buffer",
            {new_size_request: (new_size_request: int), config: (t: t)},
          )
        ],
      );
    };
    let len = Int.min(t.max_buffer_size, Int.ceil_pow2(new_size_request));
    Bigstring.unsafe_destroy_and_resize(buf, ~len);
  };
};

let set_nonblocking = fd =>
  Fd.with_file_descr_exn(fd, ignore, ~nonblocking=true);

module Reader_internal = {
  [@deriving (sexp_of, fields)]
  type t = {
    fd: Fd.t,
    config: Config.t,
    mutable reading: bool,
    mutable closed: bool,
    close_finished: Ivar.t(unit),
    mutable buf: [@sexp.opaque] Bigstring.t,
    mutable pos:
      int, /* Start of unconsumed data. */
    mutable max: int /* End   of unconsumed data. */
  };

  let create = (fd, config) => {
    set_nonblocking(fd);
    {
      fd,
      config,
      reading: false,
      closed: false,
      close_finished: Ivar.create(),
      buf: Bigstring.create(config.initial_buffer_size),
      pos: 0,
      max: 0,
    };
  };

  let is_closed = t => t.closed;
  let close_finished = t => Ivar.read(t.close_finished);

  /* Shift remaining unconsumed data to the beginning of the buffer */
  let shift_unconsumed = t =>
    if (t.pos > 0) {
      let len = t.max - t.pos;
      if (len > 0) {
        Bigstring.blit(
          ~src=t.buf,
          ~dst=t.buf,
          ~src_pos=t.pos,
          ~dst_pos=0,
          ~len,
        );
      };
      t.pos = 0;
      t.max = len;
    };

  let refill = t => {
    shift_unconsumed(t);
    let result =
      Bigstring_unix.read_assume_fd_is_nonblocking(
        Fd.file_descr_exn(t.fd),
        t.buf,
        ~pos=t.max,
        ~len=Bigstring.length(t.buf) - t.max,
      );

    if (Unix.Syscall_result.Int.is_ok(result)) {
      switch (Unix.Syscall_result.Int.ok_exn(result)) {
      | 0 => `Eof
      | n =>
        assert(n > 0);
        t.max = t.max + n;
        `Read_some;
      };
    } else {
      switch (Unix.Syscall_result.Int.error_exn(result)) {
      | EAGAIN
      | EWOULDBLOCK
      | EINTR => `Nothing_available
      | EPIPE
      | ECONNRESET
      | EHOSTUNREACH
      | ENETDOWN
      | ENETRESET
      | ENETUNREACH
      | ETIMEDOUT => `Eof
      | error => raise([@implicit_arity] Unix.Unix_error(error, "read", ""))
      };
    };
  };

  /* To avoid allocating options in a relatively safe way. */
  module Message_len: {
    type t = pri int;

    let none: t;
    let is_some: t => bool;
    let create_exn: int => t;

    /* fails on negative ints */

    let value_exn: t => int;
  } = {
    type t = int;

    let none = (-1);
    let is_some = t => t >= 0;

    let create_exn = n =>
      if (n < 0) {
        failwithf("Message_len.create_exn of negative int: %d", n, ());
      } else {
        n;
      };

    let value_exn = t =>
      if (t < 0) {
        failwith("Message_len.value_exn of None");
      } else {
        t;
      };
  };

  /* If one full message is available, returns its length (not including the
     header). Returns [Message_len.none] otherwise. */
  let get_payload_length_of_next_available_message = t => {
    let pos = t.pos;
    let available = t.max - pos;
    if (available >= Header.length) {
      let payload_len = Header.unsafe_get_payload_length(t.buf, ~pos);
      let total_len = payload_len + Header.length;
      Config.check_message_size(t.config, ~payload_len);
      if (total_len <= available) {
        Message_len.create_exn(payload_len);
      } else {
        if (total_len > Bigstring.length(t.buf)) {
          t.buf =
            Config.grow_buffer(t.config, t.buf, ~new_size_request=total_len);
        };
        Message_len.none;
      };
    } else {
      Message_len.none;
    };
  };

  module Dispatcher: {
    let run:
      (
        t,
        ~on_message: (Bigstring.t, ~pos: int, ~len: int) =>
                     Handler_result.t('a),
        ~on_end_of_batch: unit => unit,
        ~read_or_peek: [ | `Peek | `Read]
      ) =>
      Deferred.t(result('a, [ | `Closed | `Eof]));

    let peek_once_without_buffering_from_socket:
      (
        t,
        ~on_message: (Bigstring.t, ~pos: int, ~len: int) =>
                     Handler_result.t('a),
        ~len: int
      ) =>
      Deferred.t(result('a, [ | `Closed | `Not_enough_data]));
  } = {
    /* This module does a [Fd.every_ready_to] and takes care of exiting it when the
       callback returns [Wait _]. */

    type state('a) =
      | Running
      | Stopped(stop_reason('a))

    and stop_reason('a) =
      | Handler_raised
      | Eof_reached
      /* Last handler call that wasn't determined immediately */
      | Waiting_for_handler(Deferred.t(unit))
      | Stopped_by_user('a);

    type nonrec t('a) = {
      reader: t,
      on_message: (Bigstring.t, ~pos: int, ~len: int) => Handler_result.t('a),
      on_end_of_batch: unit => unit,
      interrupt:
        Ivar.t(unit), /* To stop watching the file descriptor */
      mutable state: state('a),
    };

    let is_running = t =>
      switch (t.state) {
      | Running => true
      | Stopped(_) => false
      };

    let interrupt = (t, reason) => {
      assert(is_running(t));
      t.state = Stopped(reason);
      Ivar.fill(t.interrupt, ());
    };

    let can_process_message = t => !t.reader.closed && is_running(t);

    let rec process_received_messages = (t, ~read_or_peek) =>
      if (can_process_message(t)) {
        let len = get_payload_length_of_next_available_message(t.reader);
        if (Message_len.is_some(len)) {
          let len = Message_len.value_exn(len);
          let start = t.reader.pos + Header.length;
          let () =
            switch (read_or_peek) {
            | `Read => t.reader.pos = start + len
            | `Peek => ()
            };

          switch (t.on_message(t.reader.buf, ~pos=start, ~len)) {
          | Stop(x) => interrupt(t, Stopped_by_user(x))
          | Continue => process_received_messages(t, ~read_or_peek)
          | Wait(d) =>
            if (Deferred.is_determined(d)) {
              process_received_messages(t, ~read_or_peek);
            } else {
              interrupt(t, Waiting_for_handler(d));
            }
          };
        } else {
          t.on_end_of_batch();
        };
      };

    let process_incoming = (t, ~read_or_peek) =>
      if (can_process_message(t)) {
        switch (refill(t.reader)) {
        | `Eof => interrupt(t, Eof_reached)
        | `Nothing_available => ()
        | `Read_some => process_received_messages(t, ~read_or_peek)
        };
      };

    /* We want to stop reading/dispatching as soon as we get an error */
    let stop_watching_on_error = (t, ~monitor) => {
      let parent = Monitor.current();
      Monitor.detach_and_iter_errors(
        monitor,
        ~f=exn => {
          if (is_running(t)) {
            interrupt(t, Handler_raised);
          };
          /* Let the monitor in effect when [dispatch] was called deal with the error. */
          Monitor.send_exn(parent, exn);
        },
      );
    };

    let rec run = (reader, ~on_message, ~on_end_of_batch, ~read_or_peek) => {
      let t = {
        reader,
        interrupt: Ivar.create(),
        state: Running,
        on_message,
        on_end_of_batch,
      };

      let monitor =
        Monitor.create(
          ~here=[%here],
          ~name="Rpc_transport_low_latency.Reader_internal.Dispatcher.run",
          (),
        );

      stop_watching_on_error(t, ~monitor);
      switch%bind (
        Scheduler.within'(
          ~monitor,
          () => {
            /* Process messages currently in the buffer. */
            /* This will fill [t.interrupt] if [on_message] returns [Wait _]. However, we
               expect [on_message] to almost never return [Wait _] with this transport, since
               even the "non-copying" writes return [Deferred.unit]. */
            process_received_messages(t, ~read_or_peek);
            let interrupt =
              Deferred.any([
                Ivar.read(t.interrupt),
                close_finished(t.reader),
              ]);

            Fd.interruptible_every_ready_to(
              ~interrupt,
              t.reader.fd,
              `Read,
              process_incoming(~read_or_peek),
              t,
            );
          },
        )
      ) {
      | `Bad_fd
      | `Unsupported =>
        failwith(
          "Rpc_transport_low_latency.Reader.read_forever: file descriptor doesn't support watching",
        )
      | `Closed
      | `Interrupted =>
        switch (t.state) {
        | Running =>
          assert(Fd.is_closed(t.reader.fd) || t.reader.closed);
          return(Error(`Closed));
        | Stopped(Stopped_by_user(x)) => return(Ok(x))
        | Stopped(Handler_raised) =>
          /* The exception has been propagated, we only arrive here because we forced the
             [interruptible_every_ready_to] to be interrupted. */
          Deferred.never()
        | Stopped(Eof_reached) => return(Error(`Eof))
        | Stopped(Waiting_for_handler(d)) =>
          let%bind () = d;
          if (reader.closed) {
            return(Error(`Closed));
          } else {
            run(reader, ~on_message, ~on_end_of_batch, ~read_or_peek);
          };
        }
      };
    };

    let peek_once_without_buffering_from_socket = (reader, ~on_message, ~len) => {
      let t = {
        reader,
        interrupt: Ivar.create(),
        state: Running,
        on_message,
        on_end_of_batch: ignore,
      };

      let monitor =
        Monitor.create(
          ~here=[%here],
          ~name=
            "Rpc_transport_low_latency.Reader_internal.Dispatcher.peek_once_without_buffering_from_socket",
          (),
        );

      let buf = Bigstring.create(len);
      stop_watching_on_error(t, ~monitor);
      switch%bind (
        Scheduler.within'(~monitor, () =>
          Fd.interruptible_every_ready_to(
            ~interrupt=
              Deferred.any([
                Ivar.read(t.interrupt),
                close_finished(t.reader),
              ]),
            t.reader.fd,
            `Read,
            t =>
              if (can_process_message(t)) {
                let peek_len =
                  /* [Fd.syscall_exn] catches EINTR and retries the function. This is better
                        than calling [recv_peek_assume_fd_is_nonblocking] directly.
                     */
                  Fd.syscall_exn(t.reader.fd, file_descr =>
                    Bigstring_unix.recv_peek_assume_fd_is_nonblocking(
                      file_descr,
                      ~pos=0,
                      ~len,
                      buf,
                    )
                  );

                if (peek_len >= len) {
                  switch (on_message(buf, ~pos=0, ~len)) {
                  | Stop(x) => interrupt(t, Stopped_by_user(x))
                  | Continue
                  | Wait(_) =>
                    failwith(
                      "Rpc_transport_low_latency.Reader_internal.Dispatcher.peek_once_without_buffering_from_socket: on_message returned unexpected value",
                    )
                  };
                } else {
                  interrupt(t, Eof_reached);
                };
              },
            t,
          )
        )
      ) {
      | `Bad_fd
      | `Unsupported =>
        failwith(
          "Rpc_transport_low_latency.Reader_internal.Dispatcher.peek_once_without_buffering_from_socket file descriptor doesn't support watching",
        )
      | `Closed
      | `Interrupted =>
        switch (t.state) {
        | Running =>
          assert(Fd.is_closed(t.reader.fd) || t.reader.closed);
          return(Error(`Closed));
        | Stopped(Stopped_by_user(x)) => return(Ok(x))
        | Stopped(Eof_reached) => return(Error(`Not_enough_data))
        | Stopped(Handler_raised) =>
          /* The exception has been propagated, we only arrive here because we forced the
             [interruptible_every_ready_to] to be interrupted. */
          Deferred.never()
        | Stopped(Waiting_for_handler(_)) =>
          failwith(
            "Rpc_transport_low_latency.Reader_internal.Dispatcher.peek_once_without_buffering_from_socket: unexpected state Waiting_for_handler",
          )
        }
      };
    };
  };

  let read_or_peek_dispatcher = (t, ~dispatcher_impl, ~caller_name) => {
    if (t.closed) {
      failwiths(
        ~here=[%here],
        "Rpc_transport_low_latency.Reader: reader closed",
        "",
        [%sexp_of: string],
      );
    };
    if (t.reading) {
      failwiths(
        ~here=[%here],
        "Rpc_transport_low_latency.Reader: already reading",
        "",
        [%sexp_of: string],
      );
    };
    t.reading = true;
    Monitor.protect(
      ~run=`Now,
      ~rest=`Raise,
      ~here=[%here],
      ~name=caller_name,
      ~finally=
        () => {
          t.reading = false;
          Deferred.unit;
        },
      () => dispatcher_impl(),
    );
  };

  let read_forever = (t, ~on_message, ~on_end_of_batch) =>
    read_or_peek_dispatcher(
      t,
      ~dispatcher_impl=
        () =>
          Dispatcher.run(
            t,
            ~on_message,
            ~on_end_of_batch,
            ~read_or_peek=`Read,
          ),
      ~caller_name="Rpc_transport_low_latency.Reader_internal.read_forever",
    );

  let peek_bin_prot = (t, bin_reader: Bin_prot.Type_class.reader(_)) => {
    let on_message = (buf, ~pos, ~len) => {
      let pos_ref = ref(pos);
      let x = bin_reader.read(buf, ~pos_ref);
      if (pos_ref^ != pos + len) {
        failwithf(
          "peek_bin_prot: message length (%d) did not match expected length (%d)",
          pos_ref^ - pos,
          len,
          (),
        );
      } else {
        Handler_result.Stop(x);
      };
    };

    read_or_peek_dispatcher(
      t,
      ~dispatcher_impl=
        () =>
          Dispatcher.run(
            t,
            ~on_message,
            ~on_end_of_batch=ignore,
            ~read_or_peek=`Peek,
          ),
      ~caller_name="Rpc_transport_low_latency.Reader_internal.peek_bin_prot",
    );
  };

  let peek_once_without_buffering_from_socket = (t, ~len) => {
    let on_message = (buf, ~pos as _, ~len as _) => Handler_result.Stop(buf);
    read_or_peek_dispatcher(
      t,
      ~dispatcher_impl=
        () =>
          Dispatcher.peek_once_without_buffering_from_socket(
            t,
            ~on_message,
            ~len,
          ),
      ~caller_name=
        "Rpc_transport_low_latency.Reader_internal.peek_once_without_buffering_from_socket",
    );
  };

  let close = t => {
    if (!t.closed) {
      t.closed = true;
      Fd.close(t.fd) >>> (() => Ivar.fill(t.close_finished, ()));
    };
    close_finished(t);
  };
};

module Writer_internal = {
  [@deriving sexp_of]
  type flush = {
    pos: Int63.t,
    ivar: Ivar.t(unit),
  };

  let get_job_number = () => Scheduler.num_jobs_run();

  module Connection_state: {
    [@deriving sexp_of]
    type t;

    let create: unit => t;
    let is_currently_accepting_writes: t => bool;
    let is_able_to_send_data: t => bool;
    let start_close: t => unit;
    let finish_close: (t, ~fd_closed: Deferred.t(unit)) => unit;
    let connection_lost: t => unit;
    let close_finished: t => Deferred.t(unit);
    let stopped: t => Deferred.t(unit);
  } = {
    [@deriving sexp_of]
    type t = {
      close_started: Ivar.t(unit),
      close_finished: Ivar.t(unit),
      connection_lost: Ivar.t(unit),
    };

    let start_close = t => Ivar.fill_if_empty(t.close_started, ());

    let finish_close = (t, ~fd_closed) => {
      start_close(t);
      Ivar.fill_if_empty(t.connection_lost, ());
      upon(fd_closed, Ivar.fill_if_empty(t.close_finished));
    };

    let close_finished = t => Ivar.read(t.close_finished);
    let is_currently_accepting_writes = t => Ivar.is_empty(t.close_started);
    let is_able_to_send_data = t => Ivar.is_empty(t.connection_lost);
    let connection_lost = t => Ivar.fill_if_empty(t.connection_lost, ());

    let stopped = t =>
      Deferred.any([
        Ivar.read(t.connection_lost),
        Ivar.read(t.close_started),
      ]);

    let create = () => {
      close_started: Ivar.create(),
      close_finished: Ivar.create(),
      connection_lost: Ivar.create(),
    };
  };

  [@deriving (sexp_of, fields)]
  type t = {
    fd: Fd.t,
    config: Config.t,
    connection_state: Connection_state.t,
    mutable writing: bool,
    mutable buf: [@sexp.opaque] Bigstring.t,
    mutable pos: int,
    mutable bytes_written: Int63.t,
    monitor: Monitor.t,
    flushes:
      Queue.t(flush), /* the job number of the job when we last sent data */
    mutable last_send_job: int,
    mutable sends_in_this_job: int,
  };

  let create = (fd, config) => {
    set_nonblocking(fd);
    {
      fd,
      config,
      writing: false,
      connection_state: Connection_state.create(),
      buf: Bigstring.create(config.initial_buffer_size),
      pos: 0,
      bytes_written: Int63.zero,
      monitor: Monitor.create(),
      flushes: Queue.create(),
      last_send_job: 0,
      sends_in_this_job: 0,
    };
  };

  let is_closed = t =>
    !Connection_state.is_currently_accepting_writes(t.connection_state);

  let close_finished = t =>
    Connection_state.close_finished(t.connection_state);
  let bytes_to_write = t => t.pos;
  let stopped = t => Connection_state.stopped(t.connection_state);

  let flushed = t =>
    if (t.pos == 0) {
      Deferred.unit;
    } else if (!Connection_state.is_able_to_send_data(t.connection_state)) {
      Deferred.never();
    } else {
      let flush = {
        pos: Int63.(+)(t.bytes_written, Int63.of_int(t.pos)),
        ivar: Ivar.create(),
      };

      Queue.enqueue(t.flushes, flush);
      Ivar.read(flush.ivar);
    };

  let ready_to_write = flushed;

  let dequeue_flushes = t =>
    while (!Queue.is_empty(t.flushes)
           && Int63.(<=)(Queue.peek_exn(t.flushes).pos, t.bytes_written)) {
      Ivar.fill(Queue.dequeue_exn(t.flushes).ivar, ());
    };

  /* Discard the [n] first bytes of [t.buf] */
  let discard = (t, n) => {
    assert(n >= 0 && n <= t.pos);
    let remaining = t.pos - n;
    if (remaining > 0) {
      Bigstring.blit(
        ~src=t.buf,
        ~dst=t.buf,
        ~src_pos=n,
        ~dst_pos=0,
        ~len=remaining,
      );
    };
    t.pos = remaining;
    t.bytes_written = Int63.(+)(t.bytes_written, Int63.of_int(n));
    dequeue_flushes(t);
  };

  module Error_kind = {
    type t =
      | Write_blocked
      | Connection_lost
      | Other_error;
  };

  let handle_error = (t, error: Unix.Error.t): Error_kind.t =>
    switch (error) {
    | EAGAIN
    | EWOULDBLOCK
    | EINTR => Write_blocked
    | EPIPE
    | ECONNRESET
    | EHOSTUNREACH
    | ENETDOWN
    | ENETRESET
    | ENETUNREACH
    | ETIMEDOUT =>
      Connection_state.connection_lost(t.connection_state);
      Connection_lost;
    | _ => Other_error
    };

  module Single_write_result = {
    type t =
      | Continue
      | Stop;
  };

  let single_write = (t): Single_write_result.t =>
    switch (
      Bigstring_unix.write_assume_fd_is_nonblocking(
        Fd.file_descr_exn(t.fd),
        t.buf,
        ~pos=0,
        ~len=t.pos,
      )
    ) {
    | n =>
      discard(t, n);
      Continue;
    | exception ([@implicit_arity] Unix.Unix_error(error, _, _) as exn) =>
      switch (handle_error(t, error)) {
      | Write_blocked => Continue
      | Connection_lost => Stop
      | Other_error => raise(exn)
      }
    };

  let finish_close = t => {
    let fd_closed = Fd.close(t.fd);
    t.writing = false;
    Connection_state.finish_close(t.connection_state, ~fd_closed);
  };

  let rec write_everything = t =>
    switch (single_write(t)) {
    | Stop => finish_close(t)
    | Continue =>
      if (t.pos == 0) {
        t.writing = false;
        if (is_closed(t)) {
          finish_close(t);
        };
      } else {
        wait_and_write_everything(t);
      }
    }

  and wait_and_write_everything = t =>
    Clock_ns.with_timeout(t.config.write_timeout, Fd.ready_to(t.fd, `Write))
    >>> (
      result =>
        if (!Connection_state.is_able_to_send_data(t.connection_state)) {
          finish_close(t);
        } else {
          switch (result) {
          | `Result(`Ready) => write_everything(t)
          | `Timeout =>
            Log.Global.sexp(
              ~level=`Error,
              [%message
                "Rpc_transport_low_latency.Writer timed out waiting to write on file descriptor. Closing the writer."(
                  ~timeout=t.config.write_timeout: Time_ns.Span.t,
                  t: t,
                )
              ],
            );
            finish_close(t);
          | `Result((`Bad_fd | `Closed) as result) =>
            raise_s(
              [%sexp
                (
                  "Rpc_transport_low_latency.Writer: fd changed",
                  {
                    t: (t: t),
                    ready_to_result: (result: [ | `Bad_fd | `Closed]),
                  },
                )
              ],
            )
          };
        }
    );

  let flush = t =>
    if (!t.writing && t.pos > 0) {
      t.writing = true;
      Scheduler.within(~monitor=t.monitor, () => write_everything(t));
    };

  let schedule_flush = t =>
    if (!t.writing && t.pos > 0) {
      t.writing = true;
      Scheduler.within(~monitor=t.monitor, () =>
        wait_and_write_everything(t)
      );
    };

  let ensure_at_least = (t, ~needed) =>
    if (Bigstring.length(t.buf) - t.pos < needed) {
      let new_size_request = t.pos + needed;
      t.buf = Config.grow_buffer(t.config, t.buf, ~new_size_request);
    };

  let copy_bytes = (t, ~buf, ~pos, ~len) =>
    if (len > 0) {
      ensure_at_least(t, ~needed=len);
      Bigstring.blit(
        ~src=buf,
        ~dst=t.buf,
        ~src_pos=pos,
        ~dst_pos=t.pos,
        ~len,
      );
      t.pos = t.pos + len;
    };

  /* Write what's in the internal buffer + bytes denoted by [(buf, pos, len)] */
  let unsafe_send_bytes = (t, ~buf, ~pos, ~len) => {
    let result =
      writev2(
        Fd.file_descr_exn(t.fd),
        ~buf1=t.buf,
        ~pos1=0,
        ~len1=t.pos,
        ~buf2=buf,
        ~pos2=pos,
        ~len2=len,
      );

    if (Unix.Syscall_result.Int.is_ok(result)) {
      let n = Unix.Syscall_result.Int.ok_exn(result);
      if (n <= t.pos) {
        /* We wrote less than what's in the internal buffer, discard what was written and
           copy in the other buffer. */
        discard(t, n);
        copy_bytes(t, ~buf, ~pos, ~len);
      } else {
        let written_from_other_buf = n - t.pos;
        let remaining_in_other_buf = len - written_from_other_buf;
        discard(t, t.pos);
        if (remaining_in_other_buf > 0) {
          copy_bytes(
            t,
            ~buf,
            ~pos=pos + written_from_other_buf,
            ~len=remaining_in_other_buf,
          );
        };
      };
    } else {
      let error = Unix.Syscall_result.Int.error_exn(result);
      switch (handle_error(t, error)) {
      | Write_blocked => copy_bytes(t, ~buf, ~pos, ~len)
      | Connection_lost => ()
      | Other_error =>
        let syscall =
          if (len == 0) {
            "write";
          } else {
            "writev";
          };
        Monitor.send_exn(
          t.monitor,
          [@implicit_arity] Unix.Unix_error(error, syscall, ""),
        );
      };
    };
  };

  let slow_write_bin_prot_and_bigstring =
      (t, writer: Bin_prot.Type_class.writer(_), msg, ~buf, ~pos, ~len)
      : Send_result.t(_) => {
    let payload_len = writer.size(msg) + len;
    let total_len = Header.length + payload_len;
    if (Config.message_size_ok(t.config, ~payload_len)) {
      ensure_at_least(t, ~needed=total_len);
      Header.unsafe_set_payload_length(t.buf, ~pos=t.pos, payload_len);
      let stop = writer.write(t.buf, ~pos=t.pos + Header.length, msg);
      assert(stop + len == t.pos + total_len);
      Bigstring.blit(~src=buf, ~dst=t.buf, ~src_pos=pos, ~dst_pos=stop, ~len);
      t.pos = stop + len;
      Sent();
    } else {
      Message_too_big({
        size: payload_len,
        max_message_size: t.config.max_message_size,
      });
    };
  };

  let should_send_now = t => {
    let current_job = get_job_number();
    if (current_job == t.last_send_job) {
      t.sends_in_this_job = t.sends_in_this_job + 1;
    } else {
      t.last_send_job = current_job;
      t.sends_in_this_job = 1;
    };
    t.pos >= t.config.buffering_threshold_in_bytes
    || t.sends_in_this_job <= t.config.start_batching_after_num_messages;
  };

  let send_bin_prot_and_bigstring =
      (t, writer: Bin_prot.Type_class.writer(_), msg, ~buf, ~pos, ~len)
      : Send_result.t(_) =>
    if (is_closed(t)) {
      Closed;
    } else {
      Ordered_collection_common.check_pos_len_exn(
        ~pos,
        ~len,
        ~total_length=Bigstring.length(buf),
      );
      if (Connection_state.is_able_to_send_data(t.connection_state)) {
        let send_now = should_send_now(t);
        let result =
          if (Bigstring.length(t.buf) - t.pos < Header.length) {
            slow_write_bin_prot_and_bigstring(
              t,
              writer,
              msg,
              ~buf,
              ~pos,
              ~len,
            );
          } else {
            switch (writer.write(t.buf, ~pos=t.pos + Header.length, msg)) {
            | exception _ =>
              /* It's likely that the exception is due to a buffer overflow, so resize the
                 internal buffer and try again. Technically we could match on
                 [Bin_prot.Common.Buffer_short] only, however we can't easily enforce that
                 custom bin_write_xxx functions raise this particular exception and not
                 [Invalid_argument] or [Failure] for instance. */
              slow_write_bin_prot_and_bigstring(
                t,
                writer,
                msg,
                ~buf,
                ~pos,
                ~len,
              )
            | stop =>
              let payload_len = stop - (t.pos + Header.length) + len;
              if (Config.message_size_ok(t.config, ~payload_len)) {
                Header.unsafe_set_payload_length(
                  t.buf,
                  ~pos=t.pos,
                  payload_len,
                );
                t.pos = stop;
                if (send_now) {
                  let len =
                    if (len < 128) {
                      copy_bytes(t, ~buf, ~pos, ~len);
                      0;
                    } else {
                      len;
                    };

                  unsafe_send_bytes(t, ~buf, ~pos, ~len);
                } else {
                  copy_bytes(t, ~buf, ~pos, ~len);
                };
                Sent();
              } else {
                Message_too_big({
                  size: payload_len,
                  max_message_size: t.config.max_message_size,
                });
              };
            };
          };

        if (send_now) {
          flush(t);
        } else {
          schedule_flush(t);
        };
        result;
      } else {
        Sent();
      };
    };

  let sent_deferred_unit = Send_result.Sent(Deferred.unit);

  let send_bin_prot_and_bigstring_non_copying =
      (t, writer, msg, ~buf, ~pos, ~len) =>
    switch (send_bin_prot_and_bigstring(t, writer, msg, ~buf, ~pos, ~len)) {
    | Sent () => sent_deferred_unit
    | (Closed | Message_too_big(_)) as r => r
    };

  let dummy_buf = Bigstring.create(0);

  let send_bin_prot = (t, writer, msg) =>
    send_bin_prot_and_bigstring(
      t,
      writer,
      msg,
      ~buf=dummy_buf,
      ~pos=0,
      ~len=0,
    );

  let close = t => {
    if (!is_closed(t)) {
      Connection_state.start_close(t.connection_state);
      flush(t);
      if (!t.writing) {
        finish_close(t);
      };
    };
    close_finished(t);
  };
};

let make_create = (f, ~config=Config.default, ~max_message_size, fd) => {
  let max_message_size = min(config.max_message_size, max_message_size);
  let config = Config.validate({...config, max_message_size});
  f(fd, config);
};

module Reader = {
  include Kernel_transport.Reader;

  module With_internal_reader = {
    type t = {
      internal_reader: Reader_internal.t,
      reader: Kernel_transport.Reader.t,
    };

    let create_internal = (fd, config) => {
      let internal_reader = Reader_internal.create(fd, config);
      let reader = pack((module Reader_internal), internal_reader);
      {internal_reader, reader};
    };

    let create = make_create(create_internal);
    let transport_reader = t => t.reader;

    let peek_bin_prot = (t, bin_reader) =>
      Reader_internal.peek_bin_prot(t.internal_reader, bin_reader);

    let peek_once_without_buffering_from_socket = (t, ~len) =>
      Reader_internal.peek_once_without_buffering_from_socket(
        t.internal_reader,
        ~len,
      );
  };

  let create = (~config=?, ~max_message_size, fd) => {
    let internal =
      make_create(
        With_internal_reader.create_internal,
        ~config?,
        ~max_message_size,
        fd,
      );

    internal.reader;
  };
};

module Writer = {
  include Kernel_transport.Writer;

  let create_internal = (fd, config) =>
    pack((module Writer_internal), Writer_internal.create(fd, config));

  let create = make_create(create_internal);
};

[@deriving sexp_of]
type t =
  Kernel_transport.t = {
    reader: Reader.t,
    writer: Writer.t,
  };

module With_internal_reader = {
  type t = {
    reader_with_internal_reader: Reader.With_internal_reader.t,
    writer: Writer.t,
  };

  let create_internal = (fd, config) => {
    reader_with_internal_reader:
      Reader.With_internal_reader.create_internal(fd, config),
    writer: Writer.create_internal(fd, config),
  };

  let create = make_create(create_internal);
};

let close = Kernel_transport.close;

let create = (~config=?, ~max_message_size, fd) => {
  let internal =
    make_create(
      With_internal_reader.create_internal,
      ~config?,
      ~max_message_size,
      fd,
    );

  {
    reader: internal.reader_with_internal_reader.reader,
    writer: internal.writer,
  };
};
