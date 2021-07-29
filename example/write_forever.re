open Core;
open Async;

let () =
  upon(
    Unix.openfile("/tmp/z.foo", ~mode=[`Creat, `Wronly], ~perm=0o0666),
    fd => {
      let writer = Writer.create(fd);
      let buf = String.make(1_048_576, ' ');
      let rec loop = i => {
        eprintf("about to write %d\n", i);
        Writer.write(writer, buf);
        upon(Clock.after(sec(1.)), () => loop(i + 1));
      };

      loop(0);
    },
  );

let () = never_returns(Scheduler.go());
