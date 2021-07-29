open Core;
open Async;

let (in_fd, out_fd) = Unix.socketpair();

let () =
  upon(
    Unix.close(in_fd),
    () => {
      let w = Writer.create(out_fd);
      Writer.write(w, "hello\n");
    },
  );

let () = never_returns(Scheduler.go());
