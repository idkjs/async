open Core;
open Async;

let run_test = (~fill_before_upon, ~no_ivars, ~spawn_factor) => {
  let spawn = {
    let spawns = List.range(0, spawn_factor);
    i => List.iter(spawns, ~f=_ => upon(Ivar.read(i), Fn.ignore));
  };

  let finished = Ivar.create();
  let rec loop = n =>
    if (n > 0) {
      let i = Ivar.create();
      if (fill_before_upon) {
        Ivar.fill(i, ());
        spawn(i);
      } else {
        spawn(i);
        Ivar.fill(i, ());
      };
      loop(n - 1);
    } else {
      Ivar.fill(finished, ());
    };

  loop(no_ivars);
  Ivar.read(finished);
};

let () = {
  let no_ivars = int_of_string(Sys.get_argv()[1]);
  let spawn_factor = int_of_string(Sys.get_argv()[2]);
  if (spawn_factor >= 20) {
    failwith("spawn_factor must be less than 20");
  };
  let fill_before_upon =
    switch (Sys.get_argv()[3]) {
    | "fill" => true
    | "upon" => false
    | _ => failwith("must specify either 'fill' or 'upon'")
    };

  let start = Time.now();
  upon(
    run_test(~fill_before_upon, ~no_ivars, ~spawn_factor),
    () => {
      let stop = Time.now();
      Core.Printf.printf(
        "elapsed time: %s\n",
        Time.Span.to_string(Time.diff(stop, start)),
      );
      Shutdown.shutdown(0);
    },
  );
  never_returns(Scheduler.go());
};
