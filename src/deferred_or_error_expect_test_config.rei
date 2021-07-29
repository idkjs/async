include

    Expect_test_config_types.S with
      type IO_flush.t('a) = Async_kernel.Deferred.Or_error.t('a) with
    type IO_run.t('a) = Async_kernel.Deferred.Or_error.t('a);
