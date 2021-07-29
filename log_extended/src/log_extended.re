open Core;
open Async;

include Log;

module Console = {
  module Ansi = Console.Ansi;

  let with_style = (~debug, ~info, ~error, msg) => {
    let (style, prefix) =
      switch (Log.Message.level(msg)) {
      | None => (info, "")
      | Some(`Debug) => (debug, "[DEBUG]")
      | Some(`Info) => (info, " [INFO]")
      | Some(`Error) => (error, "[ERROR]")
      };

    String.concat(~sep=" ", [prefix, Log.Message.message(msg)])
    |> Ansi.string_with_attr(style);
  };

  let output =
      (
        ~debug=([`Yellow] :> list(Ansi.attr)),
        ~info=([] :> list(Ansi.attr)),
        ~error=([`Red] :> list(Ansi.attr)),
        writer,
      ) =>
    Log.Output.create(
      ~flush=() => return(),
      msgs => {
        Queue.iter(msgs, ~f=msg =>
          with_style(~debug, ~info, ~error, msg)
          |> (
            styled_msg => {
              Writer.write(writer, styled_msg);
              Writer.newline(writer);
            }
          )
        );
        Deferred.any_unit([
          Writer.flushed(writer),
          Writer.consumer_left(writer),
        ]);
      },
    );

  module Blocking = {
    let output =
        (
          ~debug=([`Yellow] :> list(Ansi.attr)),
          ~info=([] :> list(Ansi.attr)),
          ~error=([`Red] :> list(Ansi.attr)),
          outc,
        ) =>
      Log.Blocking.Output.create(msg =>
        with_style(~debug, ~info, ~error, msg)
        |> (line => Out_channel.output_lines(outc, [line]))
      );
  };
};

module Syslog = {
  let to_syslog = msg => {
    let prefix =
      switch (Log.Message.level(msg)) {
      | None => ""
      | Some(l) => Log.Level.to_string(l) ++ " "
      };

    prefix ++ Log.Message.message(msg);
  };

  let to_level = msg =>
    switch (Log.Message.level(msg)) {
    /* syslog is generally not configured to show `LOG_DEBUG */
    | None => Syslog.Level.INFO
    | Some(`Debug) => Syslog.Level.INFO
    | Some(`Info) => Syslog.Level.INFO
    | Some(`Error) => Syslog.Level.ERR
    };

  let default_options = [Syslog.Open_option.PID, Syslog.Open_option.CONS];

  let openlog = (~id=?, ~options=default_options, ~facility=?, ()) =>
    Syslog.openlog(~id?, ~options, ~facility?, ());

  let output = (~id=?, ~options=?, ~facility=?, ()) => {
    let ready = {
      let d = Ivar.create();
      /* openlog () shouldn't block by default, but In_thread.run's a
         cheap cure for paranoia */
      upon(In_thread.run(openlog(~id?, ~options?, ~facility?)), () =>
        Ivar.fill(d, ())
      );
      Ivar.read(d);
    };

    Log.Output.create(
      ~flush=() => return(),
      msgs =>
        ready
        >>= (
          () =>
            In_thread.run(() =>
              Queue.iter(
                msgs,
                ~f=msg => {
                  let syslog_level = to_level(msg);
                  let msg = to_syslog(msg);
                  Syslog.syslog(~level=syslog_level, msg ++ "\n");
                },
              )
            )
        ),
    );
  };

  module Blocking = {
    let output = () => {
      openlog();
      Log.Blocking.Output.create(msg =>
        let syslog_level = to_level(msg);
        let msg = to_syslog(msg);
        Syslog.syslog(~level=syslog_level, msg ++ "\n");
      );
    };
  };
};

module Command = {
  open Core;
  open Async;

  [@deriving sexp]
  type console_style =
    | Plain
    | Color;
  [@deriving sexp]
  type console_output =
    | No
    | Stdout(console_style)
    | Stderr(console_style);

  module Parameters = {
    [@deriving (fields, sexp)]
    type t = {
      log_level: Level.t,
      log_to_console: console_output,
      log_to_syslog: bool,
      log_to_file: option(string),
    };

    module Flag_name = {
      /* This module exists to make it easier to inspect flag names. */
      let log_to_file = "log-to-file";
      let log_to_console = "log-to-console";
      let log_to_stdout = "log-to-stdout";
      let log_with_color = "log-with-color";
      let log_to_syslog = "log-to-syslog";
      let log_level = "log-level";
    };

    let log_to_file_flag = t => {
      let default = Option.value(t.log_to_file, ~default="<NONE>");
      let doc = sprintf("FILENAME Log to a file (default: %s)", default);
      Command.Param.(
        flag(Flag_name.log_to_file, optional(Filename_unix.arg_type), ~doc)
      );
    };

    let log_to_console_flag = t => {
      let default =
        switch (t.log_to_console) {
        | No => false
        | Stderr(_)
        | Stdout(_) => true
        };

      let doc = sprintf("BOOL Log to console (default: %{Bool})"^, default);
      Command.Param.(
        flag(
          Flag_name.log_to_console,
          optional_with_default(default, bool),
          ~doc,
        )
      );
    };

    let log_to_syslog_flag = t => {
      let doc =
        sprintf("BOOL Log to syslog (default: %{Bool})"^, t.log_to_syslog);
      Command.Param.(
        flag(
          Flag_name.log_to_syslog,
          optional_with_default(t.log_to_syslog, bool),
          ~doc,
        )
      );
    };

    let log_to_stdout_flag = t => {
      let default =
        switch (t.log_to_console) {
        | No => false
        | Stdout(_) => true
        | Stderr(_) => false
        };

      let doc =
        sprintf(
          "BOOL Log to stdout when logging to console (default: %{Bool})"^,
          default,
        );

      Command.Param.(
        flag(
          Flag_name.log_to_stdout,
          optional_with_default(default, bool),
          ~doc,
        )
      );
    };

    let log_with_color_flag = t => {
      let default =
        switch (t.log_to_console) {
        | No
        | Stdout(Plain)
        | Stderr(Plain) => false
        | Stdout(Color)
        | Stderr(Color) => true
        };

      let doc =
        sprintf(
          "BOOL Log with color when logging to console (default: %{Bool})"^,
          default,
        );

      Command.Param.(
        flag(
          Flag_name.log_with_color,
          optional_with_default(default, bool),
          ~doc,
        )
      );
    };

    let log_level_flag = t => {
      let doc =
        sprintf(
          "LEVEL Set log level to one of [debug | error | info] (default: %{Log.Level})"
            ^,
          t.log_level,
        );

      Command.Param.(
        flag(
          Flag_name.log_level,
          optional_with_default(t.log_level, Level.arg),
          ~doc,
        )
      );
    };

    let create_log_to_console =
        (~log_to_console, ~log_with_color, ~log_to_stdout) =>
      switch (log_to_console, log_with_color, log_to_stdout) {
      | (false, true, _)
      | (false, _, true) =>
        failwithf(
          "-%s and -%s require -%s",
          Flag_name.log_with_color,
          Flag_name.log_to_stdout,
          Flag_name.log_to_console,
          (),
        )
      | (true, false, false) => Stderr(Plain)
      | (true, true, false) => Stderr(Color)
      | (true, false, true) => Stdout(Plain)
      | (true, true, true) => Stdout(Color)
      | (false, false, false) => No
      };

    /* This tests the only(?) path to turning input into a safe type. */
    let%expect_test "console_output" = {
      let print = (~log_to_console, ~log_with_color, ~log_to_stdout) => {
        let result =
          switch (
            create_log_to_console(
              ~log_to_console,
              ~log_with_color,
              ~log_to_stdout,
            )
            |> sexp_of_console_output
            |> Sexp.to_string
          ) {
          | exception e => Or_error.of_exn(~backtrace=`This(""), e)
          | s => Or_error.return(s)
          };

        Async.printf("%{sexp:string Or_error.t}\n"^, result);
      };

      print(
        ~log_to_console=false,
        ~log_with_color=false,
        ~log_to_stdout=false,
      );
      %expect
      {| (Ok No) |};
      print(~log_to_console=false, ~log_with_color=true, ~log_to_stdout=true);
      %expect
      {|
        (Error
         ((Failure "-log-with-color and -log-to-stdout require -log-to-console") "")) |};
      print(~log_to_console=true, ~log_with_color=true, ~log_to_stdout=false);
      %expect
      {| (Ok "(Stderr Color)") |};
      print(
        ~log_to_console=true,
        ~log_with_color=false,
        ~log_to_stdout=false,
      );
      %expect
      {| (Ok "(Stderr Plain)") |};
      print(~log_to_console=true, ~log_with_color=true, ~log_to_stdout=true);
      %expect
      {| (Ok "(Stdout Color)") |};
      print(~log_to_console=true, ~log_with_color=false, ~log_to_stdout=true);
      %expect
      {| (Ok "(Stdout Plain)") |};
      Deferred.unit;
    };

    let params = t => {
      let%map_open.Command log_to_file = log_to_file_flag(t)
      and log_to_console = log_to_console_flag(t)
      and log_to_stdout = log_to_stdout_flag(t)
      and log_with_color = log_with_color_flag(t)
      and log_to_syslog = log_to_syslog_flag(t)
      and log_level = log_level_flag(t);

      {
        log_to_file,
        log_to_console:
          create_log_to_console(
            ~log_to_console,
            ~log_with_color,
            ~log_to_stdout,
          ),
        log_to_syslog,
        log_level,
      };
    };

    let console_output = t =>
      switch (t.log_to_console) {
      | No => None
      | Stderr(Plain) => Some(Log.Output.stderr())
      | Stderr(Color) => Some(Console.output(Lazy.force(Writer.stderr)))
      | Stdout(Plain) => Some(Log.Output.stdout())
      | Stdout(Color) => Some(Console.output(Lazy.force(Writer.stdout)))
      };

    let syslog_output = t =>
      if (t.log_to_syslog) {
        Some(Syslog.output());
      } else {
        None;
      };

    let file_output = t =>
      switch (t.log_to_file) {
      | None => None
      | Some(filename) => Some(Output.file(`Text, ~filename))
      };

    let outputs = t =>
      List.filter_map(
        [console_output(t), syslog_output(t), file_output(t)],
        ~f=Fn.id,
      );
  };

  let setup_via_params =
      (
        ~default_output_level=`Info,
        ~log_to_console_by_default,
        ~log_to_syslog_by_default,
        ~log_to_file_by_default=?,
        (),
      ) => {
    let default =
      Parameters.Fields.create(
        ~log_level=default_output_level,
        ~log_to_console=log_to_console_by_default,
        ~log_to_syslog=log_to_syslog_by_default,
        ~log_to_file=log_to_file_by_default,
      );

    let%map.Command params = Parameters.params(default);
    Log.Global.set_output(Parameters.outputs(params));
    Log.Global.set_level(Parameters.log_level(params));
  };
};
