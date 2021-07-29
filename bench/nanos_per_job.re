open Core;
open Async;

let () = {
  let test = num_live_jobs => {
    Gc.compact();
    Deferred.create(finished =>
      let num_jobs = ref(0);
      let start = Time.now();
      upon(
        after(sec(5.)),
        () => {
          let elapsed = Time.diff(Time.now(), start);
          Core.eprintf(
            "num_live_jobs: %7d  nanos per job: %d\n%!",
            num_live_jobs,
            Float.iround_nearest_exn(
              Time.Span.to_ns(elapsed) /. Float.of_int(num_jobs^),
            ),
          );
          Ivar.fill(finished, ());
        },
      );
      for (_ in 1 to num_live_jobs) {
        let rec loop = () =>
          upon(
            Deferred.unit,
            () => {
              incr(num_jobs);
              if (Ivar.is_empty(finished)) {
                loop();
              };
            },
          );

        loop();
      };
    );
  };

  upon(
    Deferred.repeat_until_finished(1, num_live_jobs =>
      if (num_live_jobs > 2_000_000) {
        return(`Finished());
      } else {
        test(num_live_jobs) >>| (() => `Repeat(num_live_jobs * 2));
      }
    ),
    () =>
    shutdown(0)
  );
  never_returns(Scheduler.go());
};
