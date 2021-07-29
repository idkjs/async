open! Core;
open! Async;

let%expect_test "Flock" =
  Expect_test_helpers_async.with_temp_dir(tempdir =>
    let lock_path = tempdir ^/ "lock-file";
    let second_thread_started = Ivar.create();
    let%bind flock =
      switch%map (Lock_file_async.Flock.lock_exn(~lock_path)) {
      | `Somebody_else_took_it => assert(false)
      | `We_took_it(flock) =>
        print_endline("original thread took lock");
        flock;
      };

    let%bind () = {
      let%bind () = Ivar.read(second_thread_started);
      print_endline("original thread releasing lock");
      Lock_file_async.Flock.unlock_exn(flock);
    }
    and () = {
      let waiting_thread =
        Lock_file_async.Flock.wait_for_lock_exn(~lock_path, ());
      print_endline("waiting thread started");
      Ivar.fill(second_thread_started, ());
      let%bind flock = waiting_thread;
      print_endline("waiting thread took lock");
      let%map () = Lock_file_async.Flock.unlock_exn(flock);
      print_endline("waiting thread released lock");
    };

    %expect
    {|
      original thread took lock
      waiting thread started
      original thread releasing lock
      waiting thread took lock
      waiting thread released lock |};
    return();
  );

let%expect_test "Symlink" =
  Expect_test_helpers_async.with_temp_dir(tempdir =>
    let lock_path = tempdir ^/ "lock-symlink";
    let second_thread_started = Ivar.create();
    let%bind flock =
      switch%map (
        Lock_file_async.Symlink.lock_exn(
          ~lock_path,
          ~metadata="original-thread",
        )
      ) {
      | `Somebody_else_took_it(_) => assert(false)
      | `We_took_it(flock) =>
        print_endline("original thread took lock");
        flock;
      };

    let%bind () = {
      let%bind () = Ivar.read(second_thread_started);
      print_endline("original thread releasing lock");
      Lock_file_async.Symlink.unlock_exn(flock);
    }
    and () = {
      let%bind () =
        switch%map (
          Lock_file_async.Symlink.lock_exn(
            ~lock_path,
            ~metadata="waiting-thread",
          )
        ) {
        | `Somebody_else_took_it(metadata) =>
          print_s(
            [%sexp
              `waiting_thread_sees(
                `lock_taken_by(metadata: Or_error.t(string)),
              )
            ],
          )
        | `We_took_it(_) => assert(false)
        };

      let waiting_thread =
        Lock_file_async.Symlink.wait_for_lock_exn(
          ~lock_path,
          ~metadata="waiting-thread",
          (),
        );

      Ivar.fill(second_thread_started, ());
      let%bind flock = waiting_thread;
      print_endline("waiting thread took lock");
      let%map () = Lock_file_async.Symlink.unlock_exn(flock);
      print_endline("waiting thread released lock");
    };

    %expect
    {|
      original thread took lock
      (waiting_thread_sees (lock_taken_by (Ok original-thread)))
      original thread releasing lock
      waiting thread took lock
      waiting thread released lock |};
    return();
  );
