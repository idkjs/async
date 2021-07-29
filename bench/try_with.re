open! Core;
open! Async_kernel;

let print_sexp = sexp => Printf.printf("%s\n", sexp |> Sexp.to_string_hum);

let () = {
  Int_conversions.sexp_of_int_style := `Underscores;
  let _info = Info.of_string("foo");
  let ivars = ref([]);
  let num_iters = 1_000;
  Gc.full_major();
  let minor_before = Gc.minor_words();
  let promoted_before = Gc.promoted_words();
  for (_ in 1 to num_iters) {
    ignore(
      try_with(
        ~run=`Schedule,
        ~rest=`Log,
        () => {
          let i = Ivar.create();
          ivars := [i, ...ivars^];
          Ivar.read(i);
        },
      ),
    );
  };
  Async_kernel_scheduler.Expert.run_cycles_until_no_jobs_remain();
  Gc.full_major();
  let minor_after = Gc.minor_words();
  let promoted_after = Gc.promoted_words();
  print_sexp(
    [%sexp
      {
        minor_words: ((minor_after - minor_before) / num_iters: int),
        promoted_words: ((promoted_after - promoted_before) / num_iters: int),
      }
    ],
  );
  print_sexp(
    [%sexp {live_words: (Ocaml_value_size.words(ivars^) / num_iters: int)}],
  );
};
