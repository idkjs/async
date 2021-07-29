open! Async_kernel;

/** {2 Async_kernel} */;

/** @open */
include Async_kernel;

module Deferred = {
  include Deferred;

  module Or_error = {
    include Async_kernel.Deferred.Or_error;
    module Expect_test_config = Deferred_or_error_expect_test_config;
  };
};

/** {2 Async_unix} */;

/** @open */
include Async_unix;

/** {2 Async_command} */;

/* We define [Command] using [struct include ... end] rather than as an alias so that we
   don't have to add [async_command] to downstream jbuild library imports. */
module Command = {
  include Async_command;
};

/** {2 Async_rpc} */;

/** @open */
include Async_rpc;

/* We define [Quickcheck] using [struct include ... end] rather than as an alias so that
   we don't have to add [async_quickcheck] to downstream jbuild library imports. */
module Quickcheck = {
  include Async_quickcheck;
};

let%test "Async library initialization does not initialize the scheduler" =
  Scheduler.is_ready_to_initialize();

module Expect_test_config = Expect_test_config_with_unit_expect;

module Expect_test_config_with_unit_expect_or_error = Expect_test_config_with_unit_expect_or_error;
