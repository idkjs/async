/** [Async.Command] is {{!Core.Command}[Core.Command]} with additional Async functions. */;

open! Core;
open! Import;

/** @open */

include (module type of {
  include Core.Command;
});

type with_options('a) = (~extract_exn: bool=?) => 'a;

/** [async] is like [Core.Command.basic], except that the main function it expects returns
    [unit Deferred.t], instead of [unit].  [async] will also start the Async scheduler
    before main is run, and will stop the scheduler when main returns.

    [async] also handles top-level exceptions by wrapping the user-supplied function in a
    [Monitor.try_with]. If an exception is raised, it will print it to stderr and call
    [shutdown 1]. The [extract_exn] argument is passed along to [Monitor.try_with]; by
    default it is [false]. */

let async: with_options(basic_command(Deferred.t(unit)));

let async_spec: with_options(basic_spec_command('a, Deferred.t(unit)));

/** [async_or_error] is like [async], except that the main function it expects may
    return an error, in which case it prints out the error message and shuts down with
    exit code 1. */

let async_or_error: with_options(basic_command(Deferred.Or_error.t(unit)));

let async_spec_or_error:
  with_options(basic_spec_command('a, Deferred.Or_error.t(unit)));

/** Staged functions allow the main function to be separated into two stages.  The first
    part is guaranteed to run before the Async scheduler is started, and the second part
    will run after the scheduler is started.  This is useful if the main function runs
    code that relies on the fact that threads have not been created yet
    (e.g., [Daemon.daemonize]).

    As an example:
    {[
      let main () =
        assert (not (Scheduler.is_running ()));
        stage (fun `Scheduler_started ->
          assert (Scheduler.is_running ());
          Deferred.unit
        )
    ]}
*/;

type staged('r) = Staged.t([ | `Scheduler_started] => 'r);

module Staged: {
  let async: with_options(basic_command(staged(Deferred.t(unit))));
  let async_spec:
    with_options(basic_spec_command('a, staged(Deferred.t(unit))));
  let async_or_error:
    with_options(basic_command(staged(Deferred.Or_error.t(unit))));

  let async_spec_or_error:
    with_options(
      basic_spec_command('a, staged(Deferred.Or_error.t(unit))),
    );
};

/** To create an [Arg_type.t] that uses auto-completion and uses Async to compute the
    possible completions, one should use

    {[
      Arg_type.create ~complete of_string
    ]}

    where [complete] wraps its Async operations in [Thread_safe.block_on_async].  With
    this, the [complete] function is only called when the executable is auto-completing,
    not for ordinary execution.  This improves performance, and also means that the Async
    scheduler isn't started for ordinary execution of the command, which makes it possible
    for the command to daemonize (which requires the scheduler to not have been started).
*/;
