open Core;
open Poly;
open Async;
open Print;
module Fd = Unix.Fd;

/* The type of values we want to transmit for testing */
[@deriving bin_io]
type test = {
  a: int,
  b: float,
  c: array(bool),
  d: list((string, int64)),
  e: array(nativeint),
  f: array([ | `Foo | `Bar]),
  g: array(float),
};

/* This value is very likely larger than e.g. an order, fill, etc. */
let test = {
  a: 42,
  b: 3.1,
  c: Array.init(10, ~f=i => i mod 2 == 0),
  d: [
    ("asdf", 421311L),
    ("foo.bar", (-123412L)),
    ("asdfkasdfkjasdf", Int64.zero),
  ],
  e: Array.init(64, ~f=i => Nativeint.shift_right(Nativeint.one, i)),
  f:
    Array.init(20, ~f=i =>
      if (i mod 2 == 0) {
        `Foo;
      } else {
        `Bar;
      }
    ),
  g: Array.init(20, ~f=float),
};

/* Number of messages to send for testing */
let n_msgs = 100_000;

/* Reader reads binary protocol messages from a pipe */
let start_reader = fd => {
  let reader = Reader.create(fd);
  let rec loop = n =>
    /* All messages received */
    if (n == 0) {
      /* Make sure we now get an EOF */
      upon(
        Reader.read_bin_prot(reader, bin_reader_test),
        fun
        | `Eof => {
            print_endline("Reader success");
            shutdown(0);
          }
        | _ => assert(false),
      );
    } else {
      /* Read another binary protocol message */
      upon(
        Reader.read_bin_prot(reader, bin_reader_test),
        fun
        | `Ok(v) when v == test => loop(n - 1)
        | _ => assert(false),
      );
    };

  loop(n_msgs);
  never_returns(Scheduler.go());
};

/* Writer writes binary protocol messages to a pipe and waits for its
   reader child process to successfully finish reading */
let start_writer = (pid, fd) => {
  let writer = Writer.create(fd);
  let rec loop = n =>
    if (n == 0) {
      /* All messages sent; make sure to flush writer buffers (easy to
         forget!) and close the underlying file descriptor */
      Writer.close(writer)
      >>> (
        () => {
          let bytes_written = Writer.bytes_written(writer);
          printf(
            "Writer success: %s bytes written\n%!",
            Int63.to_string(bytes_written),
          );
          /* Wait for reader to terminate successfully */
          Unix.waitpid_exn(pid)
          >>> (
            () => {
              printf("All successful: transmitted %d messages\n", n_msgs);
              shutdown(0);
            }
          );
        }
      );
    } else {
      /* Write a value to the writer using the binary protocol */
      Writer.write_bin_prot(writer, bin_writer_test, test);
      /* It's usually a good idea performance-wise to flush if the buffer
         is close to full before putting in more messages, because
         otherwise the buffer has to be grown (assuming default size of
         131_072) - possibly without bounds. */
      if (Writer.bytes_to_write(writer) > 100_000) {
        upon(Writer.flushed(writer), _ => loop(n - 1));
      } else {
        loop(n - 1);
      };
    };

  loop(n_msgs);
  never_returns(Scheduler.go());
};

let () = {
  module Unix = Core_unix;
  /* Create pipe for communicating between reader and writer process */
  let (ifd, ofd) = Unix.pipe();
  /* Fork off reader process */
  switch (Unix.fork()) {
  | `In_the_child =>
    /* We are the reader */
    Unix.close(ofd);
    start_reader(
      Fd.create(Fd.Kind.Fifo, ifd, Info.of_string("<parent reader>")),
    );
  | `In_the_parent(pid) =>
    /* We are the writer */
    Unix.close(ifd);
    start_writer(
      pid,
      Fd.create(Fd.Kind.Fifo, ofd, Info.of_string("<child writer>")),
    );
  };
};
