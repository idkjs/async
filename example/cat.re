open Core;
open Async;
module Fd = Unix.Fd;

let cat = (~input, ~output) => {
  let reader = Reader.create(input);
  let writer = Writer.create(~raise_when_consumer_leaves=false, output);
  let buf = Bytes.create(4096);
  let rec loop = () =>
    choose([
      choice(Reader.read(reader, buf), r => `Reader(r)),
      choice(Writer.consumer_left(writer), () => `Epipe),
    ])
    >>> (
      fun
      | `Epipe => shutdown(0)
      | `Reader(r) =>
        switch (r) {
        | `Eof => Writer.flushed(writer) >>> (_ => shutdown(0))
        | `Ok(len) =>
          Writer.write_substring(
            writer,
            Substring.create(buf, ~pos=0, ~len),
          );
          loop();
        }
    );

  loop();
};

let () = cat(~input=Fd.stdin(), ~output=Fd.stdout());
let () = never_returns(Scheduler.go());
