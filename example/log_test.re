open Core;
open Async;

let main = () => {
  open Log;
  Global.set_level(`Debug);
  let rotating_file =
    Output.rotating_file(
      `Sexp,
      ~basename="/tmp/log_test/messages",
      Rotation.create(
        ~time=Time.Ofday.create(~hr=13, ~min=47, ()),
        ~keep=`At_least(3),
        ~naming_scheme=`Numbered,
        (),
      ),
    );

  Global.set_output([rotating_file]);
  let i = ref(0);
  Clock.every(
    sec(1.),
    () => {
      Global.info("%d", i^);
      incr(i);
    },
  );
};

let () = {
  main();
  never_returns(Scheduler.go());
};
