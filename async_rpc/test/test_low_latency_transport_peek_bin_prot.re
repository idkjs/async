open! Core;
open! Async;
open! Import;

let max_message_size = 1024 * 1024;
let message = 'a';

let create_reader = fd => {
  let reader_transport =
    Rpc.Low_latency_transport.With_internal_reader.create(
      ~max_message_size,
      fd,
    );

  reader_transport.reader_with_internal_reader;
};

let create_writer = fd => {
  let writer_transport =
    Rpc.Low_latency_transport.create(~max_message_size, fd);
  writer_transport.writer;
};

let socket_fds = () => {
  let (pipe_r, pipe_w) =
    Core_unix.socketpair(
      ~domain=Core_unix.PF_UNIX,
      ~kind=Core_unix.SOCK_STREAM,
      ~protocol=0,
      (),
    );

  let writer_fd =
    Fd.create(
      Socket(`Active),
      pipe_w,
      Info.of_string("low-latency-transport-writer"),
    );

  let reader_fd =
    Fd.create(
      Socket(`Active),
      pipe_r,
      Info.of_string("low-latency-transport-reader"),
    );

  (reader_fd, writer_fd);
};

let fifo_fds = () => {
  let (pipe_r, pipe_w) = Core_unix.pipe();
  let writer_fd =
    Fd.create(Fifo, pipe_w, Info.of_string("low-latency-transport-writer"));
  let reader_fd =
    Fd.create(Fifo, pipe_r, Info.of_string("low-latency-transport-reader"));
  (reader_fd, writer_fd);
};

let run_test_suite_with = (~fds, ~f) => {
  let (reader_fd, writer_fd) = fds();
  let (reader, writer) = (
    create_reader(reader_fd),
    create_writer(writer_fd),
  );
  switch (
    Rpc.Low_latency_transport.Writer.send_bin_prot(
      writer,
      bin_writer_char,
      message,
    )
  ) {
  | Sent () => f(~reader_fd, ~reader, ~writer_fd, ~writer)
  | (Closed | Message_too_big(_)) as res =>
    raise_s(
      [%message
        "write_bin_prot error for low latency transport"(
          res: Rpc.Low_latency_transport.Send_result.t(unit),
        )
      ],
    )
  };
};

let%expect_test "peeking a message shouldn't result in read offset to change" = {
  let%bind () =
    run_test_suite_with(
      ~fds=fifo_fds,
      ~f=(~reader_fd as _, ~reader, ~writer_fd as _, ~writer as _) =>
      switch%bind (
        Rpc.Low_latency_transport.Reader.With_internal_reader.peek_bin_prot(
          reader,
          bin_reader_char,
        )
      ) {
      | Error(`Eof | `Closed) =>
        raise_s([%message "Received EOF while peeking"])
      | Ok(msg) =>
        printf("peeked message is: %c\n", msg);
        /* read the message now */
        let%bind () =
          switch%bind (
            Rpc.Low_latency_transport.Reader.read_one_message_bin_prot(
              Rpc.Low_latency_transport.Reader.With_internal_reader.transport_reader(
                reader,
              ),
              bin_reader_char,
            )
          ) {
          | Error(`Eof | `Closed) =>
            raise_s([%message "Received EOF while reading"])
          | Ok(msg) =>
            printf("read message is: %c\n", msg);
            return();
          };

        return();
      }
    );

  %expect
  {|
    peeked message is: a
    read message is: a |};
  return();
};

let message_bin_prot_len = 1 + Async_rpc_kernel.Rpc.Transport.Header.length;

let%expect_test "peeking a message without buffering - peeking multiple times with new readers should return same result" = {
  let bigstring_read_bin_prot = (output_prefix, msg) =>
    switch (Bigstring.read_bin_prot(msg, bin_reader_char)) {
    | Error(err) =>
      raise_s(
        [%message
          "Error reading bin prot message from bigstring buffer"(err: Error.t)
        ],
      )
    | [@implicit_arity] Ok(c, _) =>
      printf("%s peeked message is: %c\n", output_prefix, c)
    };

  let peek_available_without_buffering = reader =>
    switch%bind (
      Rpc.Low_latency_transport.Reader.With_internal_reader.peek_once_without_buffering_from_socket(
        reader,
        ~len=message_bin_prot_len,
      )
    ) {
    | Error(`Not_enough_data | `Closed) =>
      raise_s([%message "Error while peeking"])
    | Ok(msg) => return(msg)
    };

  let%bind () =
    run_test_suite_with(
      ~fds=socket_fds,
      ~f=(~reader_fd, ~reader, ~writer_fd as _, ~writer as _) => {
        let%bind msg = peek_available_without_buffering(reader);
        bigstring_read_bin_prot("(first)", msg);
        /* create another reader and peek again */
        let another_reader = create_reader(reader_fd);
        let%bind msg = peek_available_without_buffering(another_reader);
        bigstring_read_bin_prot("(second)", msg);
        return();
      },
    );

  %expect
  {|
    (first) peeked message is: a
    (second) peeked message is: a |};
  return();
};

let%expect_test "peeking a message without buffering - trying to peek number of bytes longer than the message" = {
  let%bind () =
    run_test_suite_with(
      ~fds=socket_fds,
      ~f=(~reader_fd as _, ~reader, ~writer_fd as _, ~writer as _) =>
      switch%bind (
        Rpc.Low_latency_transport.Reader.With_internal_reader.peek_once_without_buffering_from_socket(
          reader,
          ~len=message_bin_prot_len + 1,
        )
      ) {
      | Error(`Not_enough_data) =>
        return(print_endline("Received Not_enough_data while peeking"))
      | Error(`Closed) => raise_s([%message "Got `Closed error"])
      | Ok(_) => raise_s([%message "Peeking unexpectedly succeeded"])
      }
    );

  %expect
  {|
    Received Not_enough_data while peeking |};
  return();
};
