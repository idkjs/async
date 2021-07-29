open Core;
open Async;

let bind_loop = () => {
  let rec loop = n =>
    if (n == 0) {
      return(123);
    } else {
      Deferred.unit >>= (() => loop(n - 1));
    };

  loop(15_000_000);
};

let main = () =>
  bind_loop()
  >>> (
    x => {
      Print.printf("done %d\n", x);
      Shutdown.shutdown(0);
    }
  );

let () = {
  main();
  Exn.handle_uncaught(~exit=true, never_returns(Scheduler.go()));
};
