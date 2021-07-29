open! Core;
open! Async_kernel;
open Deferred.Infix;
module Generator = Quickcheck.Generator;
module Observer = Quickcheck.Observer;
module Shrinker = Quickcheck.Shrinker;

module Configure = (Config: Quickcheck.Quickcheck_config) => {
  include Quickcheck.Configure(Config);

  let async_test =
      (
        ~seed=?,
        ~trials=default_trial_count,
        ~sexp_of=?,
        quickcheck_generator,
        ~f,
      ) => {
    let f_with_sexp =
      switch (sexp_of) {
      | None => f
      | Some(sexp_of_arg) => (
          x =>
            Deferred.Or_error.try_with(
              ~run=`Schedule, ~rest=`Log, ~extract_exn=true, () =>
              f(x)
            )
            >>| (
              fun
              | Ok () => ()
              | Error(e) =>
                Error.raise(Error.tag_arg(e, "random input", x, sexp_of_arg))
            )
        )
      };

    Sequence.delayed_fold(
      Sequence.take(random_sequence(~seed?, quickcheck_generator), trials),
      ~init=(),
      ~f=((), x, ~k) => f_with_sexp(x) >>= k,
      ~finish=Deferred.return,
    );
  };
};

include Configure(Quickcheck);
