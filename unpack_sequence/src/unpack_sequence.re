open! Core;
open! Async;
open! Import;

let input_closed_error = Error.of_string("input closed");

let input_closed_in_the_middle_of_data_error =
  Error.of_string("input closed in the middle of data");

let unpack_error = error =>
  Error.create("unpack error", error, [%sexp_of: Error.t]);

module Unpack_iter_result = {
  [@deriving sexp_of]
  type t('a) =
    | Input_closed
    | Input_closed_in_the_middle_of_data(Unpack_buffer.t('a))
    | Unpack_error(Error.t);

  let to_error: t(_) => Error.t = (
    fun
    | Input_closed => input_closed_error
    | Input_closed_in_the_middle_of_data(_) => input_closed_in_the_middle_of_data_error
    | Unpack_error(error) => unpack_error(error):
      t(_) => Error.t
  );
};

module Unpack_result = {
  [@deriving sexp_of]
  type t('a) =
    | Input_closed
    | Input_closed_in_the_middle_of_data(Unpack_buffer.t('a))
    | Output_closed
    | Unpack_error(Error.t);

  let to_error: t(_) => Error.t = (
    fun
    | Input_closed => input_closed_error
    | Input_closed_in_the_middle_of_data(_) => input_closed_in_the_middle_of_data_error
    | Output_closed => Error.of_string("output closed")
    | Unpack_error(error) => unpack_error(error):
      t(_) => Error.t
  );

  let eof = unpack_buffer =>
    switch (Unpack_buffer.is_empty(unpack_buffer)) {
    | Error(error) => Unpack_error(error)
    | Ok(true) => Input_closed
    | Ok(false) => Input_closed_in_the_middle_of_data(unpack_buffer)
    };

  let of_unpack_iter_result: Unpack_iter_result.t(_) => t(_) = (
    fun
    | Input_closed => Input_closed
    | Input_closed_in_the_middle_of_data(x) =>
      Input_closed_in_the_middle_of_data(x)
    | Unpack_error(e) => Unpack_error(e):
      Unpack_iter_result.t(_) => t(_)
  );
};

module Unpack_from = {
  type t =
    | Pipe(Pipe.Reader.t(string))
    | Reader(Reader.t);
};

module Unpack_to = {
  [@deriving sexp_of]
  type t('a) =
    | Iter('a => unit)
    | Pipe(Pipe.Writer.t('a));
};

let unpack_all =
    (~from: Unpack_from.t, ~to_: Unpack_to.t(_), ~using as unpack_buffer) => {
  let unpack_all_available =
    switch (to_) {
    | Iter(f) => (
        () =>
          switch (Unpack_buffer.unpack_iter(unpack_buffer, ~f)) {
          | Ok () => return(`Continue)
          | Error(error) =>
            return(`Stop(Unpack_result.Unpack_error(error)))
          }
      )
    | Pipe(output_writer) =>
      let f = a => {
        if (Pipe.is_closed(output_writer)) {
          /* This will cause [unpack_iter] below to return [Error], and will result in
             [Output_closed]. */
          failwith("output closed");
        };
        Pipe.write_without_pushback(output_writer, a);
      };

      (
        () =>
          switch (Unpack_buffer.unpack_iter(unpack_buffer, ~f)) {
          | Ok () => Pipe.pushback(output_writer) >>| (() => `Continue)
          | Error(error) =>
            return(
              `Stop(
                if (Pipe.is_closed(output_writer)) {
                  Unpack_result.Output_closed;
                } else {
                  Unpack_result.Unpack_error(error);
                },
              ),
            )
          }
      );
    };

  let finished_with_input =
    switch (from) {
    | Reader(input) =>
      /* In rare situations, a reader can asynchronously raise.  We'd rather not raise
         here, since we have a natural place to report the error. */
      try_with(~run=`Schedule, ~rest=`Log, () =>
        Reader.read_one_chunk_at_a_time(
          input, ~handle_chunk=(buf, ~pos, ~len) =>
          switch (Unpack_buffer.feed(unpack_buffer, buf, ~pos, ~len)) {
          | Error(error) =>
            return(`Stop(Unpack_result.Unpack_error(error)))
          | Ok () => unpack_all_available()
          }
        )
      )
      >>| (
        fun
        | Error(exn) => Unpack_result.Unpack_error(Error.of_exn(exn))
        | Ok(`Stopped(result)) => result
        | Ok(`Eof) => Unpack_result.eof(unpack_buffer)
        | Ok(`Eof_with_unconsumed_data(_)) =>
          /* not possible since we always consume everithing */
          assert(false)
      )
    | Pipe(input) =>
      Deferred.repeat_until_finished((), () =>
        Pipe.read'(input)
        >>= (
          fun
          | `Eof => return(`Finished(Unpack_result.eof(unpack_buffer)))
          | `Ok(q) =>
            switch (
              Queue.iter(q, ~f=string =>
                switch (Unpack_buffer.feed_string(unpack_buffer, string)) {
                | Ok () => ()
                | Error(error) => Error.raise(error)
                }
              )
            ) {
            | exception exn =>
              return(
                `Finished(Unpack_result.Unpack_error(Error.of_exn(exn))),
              )
            | () =>
              unpack_all_available()
              >>| (
                fun
                | `Continue => `Repeat()
                | `Stop(z) => `Finished(z)
              )
            }
        )
      )
    };

  switch (to_) {
  | Iter(_) => finished_with_input
  | Pipe(output) =>
    choose([
      choice(finished_with_input, Fn.id),
      choice(Pipe.closed(output), () => Unpack_result.Output_closed),
    ])
  };
};

let unpack_into_pipe = (~from, ~using) => {
  let (output_reader, output_writer) = Pipe.create();
  let result =
    unpack_all(~from, ~to_=Pipe(output_writer), ~using)
    >>| (
      result => {
        Pipe.close(output_writer);
        result;
      }
    );

  (output_reader, result);
};

let unpack_iter = (~from, ~using, ~f) =>
  unpack_all(~from, ~to_=Iter(f), ~using)
  >>| (
    fun
    | Input_closed => Unpack_iter_result.Input_closed
    | Input_closed_in_the_middle_of_data(x) =>
      Input_closed_in_the_middle_of_data(x)
    | Unpack_error(x) => Unpack_error(x)
    | Output_closed as t =>
      failwiths(
        ~here=[%here],
        "Unpack_sequence.unpack_iter got unexpected value",
        t,
        [%sexp_of: Unpack_result.t(_)],
      )
  );

let%test_module _ =
  (module
   {
     module Unpack_result = {
       include Unpack_result;

       let compare = (_compare_a, t1, t2) =>
         switch (t1, t2) {
         | (Input_closed, Input_closed) => 0
         | (
             Input_closed_in_the_middle_of_data(_),
             Input_closed_in_the_middle_of_data(_),
           ) => 0
         | (Output_closed, Output_closed) => 0
         | (Unpack_error(e_l), Unpack_error(e_r)) => Error.compare(e_l, e_r)
         | (
             Input_closed | Input_closed_in_the_middle_of_data(_) |
             Output_closed |
             Unpack_error(_),
             _,
           ) => (-1)
         };
     };

     let pack = (bin_writer, values) =>
       List.map(values, ~f=value =>
         Bin_prot.Utils.bin_dump(~header=true, bin_writer, value)
         |> Bigstring.to_string
       )
       |> String.concat;

     let break_into_pieces = (string, ~of_size) => {
       let rec loop = start_idx =>
         if (start_idx < String.length(string)) {
           let next_idx =
             Int.min(start_idx + of_size, String.length(string));
           let this_slice = String.slice(string, start_idx, next_idx);
           [this_slice, ...loop(next_idx)];
         } else {
           [];
         };

       loop(0);
     };

     let%test_unit _ =
       [%test_result: list(string)](
         break_into_pieces("foobarx", ~of_size=2),
         ~expect=["fo", "ob", "ar", "x"],
       );

     module Value = {
       [@deriving (bin_io, compare, sexp)]
       type t = {
         a: string,
         b: int,
       };

       let unpack_buffer = () => Unpack_buffer.create_bin_prot(bin_reader_t);

       /* Create a value unique to the seed. */
       let create = seed => {
         let char = Char.of_int_exn(seed + Char.to_int('a'));
         {a: String.make(seed, char), b: seed};
       };

       let pack = ts => pack(bin_writer_t, ts);

       /* Bogus bin prot data that we know will *fail* when unpacked as a [Value.t]. */
       let bogus_data = {
         let bogus_size = 10;
         let buf =
           Bigstring.init(
             Bin_prot.Utils.size_header_length + bogus_size,
             ~f=const('\000'),
           );

         ignore(
           Bin_prot.Utils.bin_write_size_header(buf, ~pos=0, bogus_size): int,
         );
         Bigstring.to_string(buf);
       };

       let%test_unit _ = {
         let unpack_buffer = unpack_buffer();
         ok_exn(Unpack_buffer.feed_string(unpack_buffer, bogus_data));
         let q = Queue.create();
         switch (Unpack_buffer.unpack_into(unpack_buffer, q)) {
         | Ok () => assert(false)
         | Error(_) => assert(Queue.is_empty(q))
         };
       };

       /* A partial [Value.t] bin prot, which will cause [Unpack_buffer] to expect more data
          when unpacked. */
       let partial_data =
         /* The size header should be more than 1 byte, so this is enough to make unpack
            wait for more data. */
         String.make(1, ' ');

       let%test_unit _ = {
         let unpack_buffer = unpack_buffer();
         ok_exn(Unpack_buffer.feed_string(unpack_buffer, partial_data));
         let q = Queue.create();
         switch (Unpack_buffer.unpack_into(unpack_buffer, q)) {
         | Ok () => assert(Queue.is_empty(q))
         | Error(_) => assert(false)
         };
       };
     };

     let values = n => List.init(n, ~f=Value.create);
     let test_size = 50;

     let (>>=) = (deferred, f) => {
       let timeout = sec(10.);
       Clock.with_timeout(timeout, deferred)
       >>| (
         fun
         | `Timeout =>
           failwithf(
             "unpack_sequence.ml: Deferred took more than %{Time.Span}"^,
             timeout,
             (),
           )
         | `Result(result) => result
       )
       >>= f;
     };

     let (>>|) = (deferred, f) => deferred >>= (x => return(f(x)));

     let setup_string_pipe_reader = () => {
       let (input_r, input_w) = Pipe.create();
       let (output, finished) =
         unpack_into_pipe(~from=Pipe(input_r), ~using=Value.unpack_buffer());

       return((input_w, output, finished));
     };

     let setup_iter = () => {
       let (input_r, input_w) = Pipe.create();
       let (output_r, output_w) = Pipe.create();
       let finished =
         unpack_iter(~from=Pipe(input_r), ~using=Value.unpack_buffer(), ~f=a =>
           Pipe.write_without_pushback(output_w, a)
         )
         >>| Unpack_result.of_unpack_iter_result;

       return((input_w, output_r, finished));
     };

     let setup_reader = () => {
       let pipe_info = Info.of_string("unpack sequence test");
       let (input_r, input_w) = Pipe.create();
       Reader.of_pipe(pipe_info, input_r)
       >>= (
         reader => {
           let (pipe, finished) =
             unpack_into_pipe(
               ~from=Reader(reader),
               ~using=Unpack_buffer.create_bin_prot(Value.bin_reader_t),
             );

           return((input_w, pipe, finished));
         }
       );
     };

     let run_tests = (~only_supports_output_to_pipe=false, test_fn) =>
       Thread_safe.block_on_async_exn(() =>
         Deferred.List.iter(
           [setup_reader, setup_string_pipe_reader]
           @ (
             if (only_supports_output_to_pipe) {
               [];
             } else {
               [setup_iter];
             }
           ),
           ~f=setup =>
           setup()
           >>= (
             ((input, output, finished)) => test_fn(input, output, finished)
           )
         )
       );

     let%test_unit "test various full reads" =
       run_tests((input, output, finished) =>
         Deferred.repeat_until_finished(values(test_size), values =>
           switch (values) {
           | [] =>
             Pipe.close(input);
             finished
             >>= (
               result => {
                 [%test_result: Unpack_result.t(Value.t)](
                   result,
                   ~expect=Unpack_result.Input_closed,
                 );
                 return(`Finished());
               }
             );
           | [_, ...rest] =>
             let data = Value.pack(values);
             Deferred.repeat_until_finished(1, of_size =>
               if (of_size >= String.length(data)) {
                 return(`Finished());
               } else {
                 let pieces = break_into_pieces(data, ~of_size);
                 Pipe.transfer_in_without_pushback(
                   input,
                   ~from=Queue.of_list(pieces),
                 );
                 Pipe.read_exactly(output, ~num_values=List.length(values))
                 >>| (
                   fun
                   | `Eof
                   | `Fewer(_) => assert(false)
                   | `Exactly(queue) => {
                       [%test_result: list(Value.t)](
                         Queue.to_list(queue),
                         ~expect=values,
                       );
                       `Repeat(of_size + 1);
                     }
                 );
               }
             )
             >>= (() => return(`Repeat(rest)));
           }
         )
       );

     let%test_unit "input closed in middle of read" =
       run_tests((input, output, finished) =>
         let values = values(test_size);
         let buffer = Value.pack(values) ++ Value.partial_data;
         Pipe.write_without_pushback(input, buffer);
         Pipe.read_exactly(output, ~num_values=List.length(values))
         >>= (
           fun
           | `Eof
           | `Fewer(_) => assert(false)
           | `Exactly(queue) => {
               [%test_result: list(Value.t)](
                 Queue.to_list(queue),
                 ~expect=values,
               );
               Pipe.close(input);
               finished
               >>= (
                 result => {
                   [%test_result: Unpack_result.t(Value.t)](
                     result,
                     ~expect=
                       Input_closed_in_the_middle_of_data(
                         Value.unpack_buffer(),
                       ),
                   );
                   Deferred.unit;
                 }
               );
             }
         );
       );

     let%test_unit "output pipe closed" =
       /* This test relies on detecting that the output pipe has been closed. */
       run_tests(
         ~only_supports_output_to_pipe=true,
         (_input, output, finished) => {
           Pipe.close_read(output);
           Pipe.read'(output)
           >>= (
             fun
             | `Ok(_) => assert(false)
             | `Eof =>
               finished
               >>= (
                 result => {
                   [%test_result: Unpack_result.t(Value.t)](
                     result,
                     ~expect=Output_closed,
                   );
                   Deferred.unit;
                 }
               )
           );
         },
       );

     let%test_unit "bad bin-io data" =
       run_tests((input, output, finished) =>
         let values = values(test_size);
         let buffer = Value.pack(values) ++ Value.bogus_data;
         Pipe.write_without_pushback(input, buffer);
         Pipe.read_exactly(output, ~num_values=List.length(values))
         >>= (
           fun
           | `Eof
           | `Fewer(_) => assert(false)
           | `Exactly(queue) => {
               [%test_result: list(Value.t)](
                 Queue.to_list(queue),
                 ~expect=values,
               );
               finished
               >>| (
                 fun
                 | Unpack_error(_) => ()
                 | _ => assert(false)
               );
             }
         );
       );
   });
