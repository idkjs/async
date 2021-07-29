open Core;
open Async;

module Data = {
  [@deriving sexp]
  type t = list((int, string));
};

[@deriving sexp]
type t = String.Map.t(Sexp.With_text.t(Data.t));

let edit_file = (type a, module A: Sexpable with type t = a, filename) => {
  let editor =
    switch (Sys.getenv("EDITOR")) {
    | Some(x) => x
    | None => "emacs"
    };

  let rec loop = () =>
    Unix.system_exn(String.concat([editor, " ", filename]))
    >>= (
      () =>
        Reader.file_contents(filename)
        >>= (
          text =>
            switch (Sexp.With_text.of_text(A.t_of_sexp, ~filename, text)) {
            | Ok(result) => return(Some(result))
            | Error(e) =>
              printf(
                "Unable to read data:\n%s\n",
                Error.sexp_of_t(e) |> Sexp.to_string_hum,
              );
              printf("Try again? (Y/n): ");
              Reader.read_line(Lazy.force(Reader.stdin))
              >>= (
                response => {
                  let reread =
                    switch (response) {
                    | `Eof => true
                    | `Ok(s) =>
                      switch (s |> String.lowercase |> String.strip) {
                      | "n"
                      | "no" => false
                      | _ => true
                      }
                    };

                  if (!reread) {
                    printf("Abandoning edit\n");
                    return(None);
                  } else {
                    loop();
                  };
                }
              );
            }
        )
    );

  loop();
};

let rec edit_loop = t => {
  printf("key to edit: ");
  Reader.read_line(force(Reader.stdin))
  >>= (
    fun
    | `Eof => Deferred.unit
    | `Ok(key) => {
        let current =
          switch (Map.find(t, key)) {
          | Some(x) => x
          | None =>
            Sexp.With_text.of_value([%sexp_of: list((int, string))], [])
          };

        let filename = Filename_unix.temp_file("test", ".scm");
        Writer.save(filename, ~contents=Sexp.With_text.text(current))
        >>= (
          () =>
            edit_file((module Data), filename)
            >>= (
              fun
              | None => edit_loop(t)
              | Some(updated) => {
                  let t = Map.set(t, ~key, ~data=updated);
                  printf("full sexp of map:\n");
                  printf("%s\n", sexp_of_t(t) |> Sexp.to_string_hum);
                  printf("\njust data:\n");
                  printf(
                    "%s\n",
                    Map.map(~f=Sexp.With_text.value, t)
                    |> [%sexp_of: String.Map.t(Data.t)]
                    |> Sexp.to_string_hum,
                  );
                  edit_loop(t);
                }
            )
        );
      }
  );
};

let () = {
  don't_wait_for(edit_loop(String.Map.empty));
  never_returns(Scheduler.go());
};
