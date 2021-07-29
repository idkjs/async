open! Core;
open Async;

let () =
  don't_wait_for(
    after(sec(60.))
    >>= (
      () =>
        /* At this point, we enqueue enough jobs to fully utilize the thread pool.  However,
           it will only be stuck for a small amount of time, since it was idle for the prior
           60s. */
        Deferred.List.init(~how=`Parallel, 50, ~f=_ =>
          In_thread.run(() => Core_unix.sleep(2))
        )
        >>= (
          _ => {
            shutdown(0);
            return();
          }
        )
    ),
  );

let () = never_returns(Scheduler.go());
