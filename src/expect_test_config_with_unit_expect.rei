/** An alternative to [Async.Expect_test_config] in which [%expect] has type [unit] rather
    than [unit Deferred.t].  This lets one write:

    {[
      [%expect {| |};
    ]}

    rather than:

    {[
      let%bind () = [%expect {| |}] in
    ]}

    It also means that [let%expect] cannot terminate with [%expect], and must instead
    terminate with [return ()]. */;

open! Async_kernel;

include
  Expect_test_config_types.S with type IO_flush.t('a) = 'a with
    type IO_run.t('a) = Deferred.t('a);
