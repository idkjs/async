open! Core;
open! Async;
open Core_bench;
module Ivar = Async_kernel_private.Ivar0;

let deep = _n => {
  let n = 30;
  let start = Ivar.create();
  /* ignore (Ivar.indir start); */
  let rec loop = n =>
    if (n == 0) {
      start;
    } else {
      Ivar.indir(loop(n - 1));
    };
  let _ = loop(n);
  start;
};

let deep_create = () => ignore(deep(100): Ivar.t(unit));
let deep_create_and_fill = () => Ivar.fill(deep(100): Ivar.t(unit), ());

let () =
  Command_unix.run(
    Bench.make_command([
      /* Bench.Test.create ~name:"one-create" (fun () ->
       *   ignore (Ivar.create ()));
       * Bench.Test.create ~name:"one-create-and-fill" (fun () ->
       *   Ivar.fill (Ivar.create ()) ()); */
      Bench.Test.create(~name="deep-create", deep_create),
      Bench.Test.create(~name="deep-create-and-fill", deep_create_and_fill),
    ]),
  );
