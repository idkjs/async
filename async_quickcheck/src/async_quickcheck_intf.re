open! Core;
open! Async_kernel;

module type Quickcheck_async_configured = {
  include Quickcheck.Quickcheck_configured;

  /** Like [test], but for asynchronous tests. */

  let async_test:
    (
      ~seed: Quickcheck.seed=?,
      ~trials: int=?,
      ~sexp_of: 'a => Sexp.t=?,
      Quickcheck.Generator.t('a),
      ~f: 'a => Deferred.t(unit)
    ) =>
    Deferred.t(unit);
};
