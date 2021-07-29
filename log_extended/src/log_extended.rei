open! Core;
open! Async;

include (module type of {
  include Async.Log;
});

module Console: {
  /** returns a [Log.Output.t] given optional styles (i.e. values of type [Ansi.t list])
      for each of the [`Debug], [`Info], and [`Error] log levels. The default styling is
      to display debug messages in yellow, error messages in red, and info messages
      without any additional styling.

      [create] doesn't take a [format] argument because colorized output should be read
      by humans.
  */

  let output:
    (
      ~debug: list(Console.Ansi.attr)=?,
      ~info: list(Console.Ansi.attr)=?,
      ~error: list(Console.Ansi.attr)=?,
      Writer.t
    ) =>
    Log.Output.t;

  module Blocking: {
    /** as [output] but for use with non-async logs */

    let output:
      (
        ~debug: list(Console.Ansi.attr)=?,
        ~info: list(Console.Ansi.attr)=?,
        ~error: list(Console.Ansi.attr)=?,
        Out_channel.t
      ) =>
      Log.Blocking.Output.t;
  };
};

module Syslog: {
  /** [output ()] return a Log.Output.t for use with Async.Log. */

  let output:
    (
      ~id: /** default is [Sys.argv.(0)] */ string=?,
      ~options: /** default is [[PID; CONS]] */ list(Syslog.Open_option.t)=?,
      ~facility: /** default is [USER] */ Syslog.Facility.t=?,
      unit
    ) =>
    Log.Output.t;

  module Blocking: {let output: unit => Log.Blocking.Output.t;};
};

module Command: {
  [@deriving sexp]
  type console_style =
    | Plain
    | Color;
  [@deriving sexp]
  type console_output =
    | No
    | Stdout(console_style)
    | Stderr(console_style);

  /** [setup_via_params] either sets up console, syslog, or file logging with the defaults
      passed in as parameters to this function, or overrides those defaults via the
      included command-line parameters. */

  let setup_via_params:
    (
      ~default_output_level: Log.Level.t=?,
      ~log_to_console_by_default: console_output,
      ~log_to_syslog_by_default: bool,
      ~log_to_file_by_default: string=?,
      unit
    ) =>
    Command.Param.t(unit);
};
