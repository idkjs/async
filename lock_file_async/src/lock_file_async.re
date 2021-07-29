open! Core;
open! Async;
open! Import;

let create = (~message=?, ~close_on_exec=?, ~unlink_on_exit=?, path) =>
  In_thread.run(() =>
    Lock_file_blocking.create(
      ~message?,
      ~close_on_exec?,
      ~unlink_on_exit?,
      path,
    )
  );

let create_exn = (~message=?, ~close_on_exec=?, ~unlink_on_exit=?, path) =>
  create(~message?, ~close_on_exec?, ~unlink_on_exit?, path)
  >>| (
    b =>
      if (!b) {
        failwiths(
          ~here=[%here],
          "Lock_file.create",
          path,
          [%sexp_of: string],
        );
      }
  );

let random = lazy(Random.State.make_self_init(~allow_in_tests=true, ()));

let repeat_with_abort = (~abort, ~f) =>
  Deferred.repeat_until_finished((), () =>
    f()
    >>= (
      fun
      | true => return(`Finished(`Ok))
      | false => {
          let delay = sec(Random.State.float(Lazy.force(random), 0.3));
          choose([
            choice(after(delay), () => `Repeat),
            choice(abort, () => `Abort),
          ])
          >>| (
            fun
            | `Abort => `Finished(`Aborted)
            | `Repeat => `Repeat()
          );
        }
    )
  );

let fail_on_abort = (path, ~held_by) =>
  fun
  | `Ok => ()
  | `Aborted =>
    failwiths(
      ~here=[%here],
      "Lock_file timed out waiting for existing lock",
      path,
      path =>
      switch (held_by) {
      | None =>
        %sexp
        (path: string)
      | Some(held_by) =>
        %sexp
        {lock: (path: string), held_by: (held_by: Sexp.t)}
      }
    );

let waiting_create =
    (
      ~abort=Deferred.never(),
      ~message=?,
      ~close_on_exec=?,
      ~unlink_on_exit=?,
      path,
    ) =>
  repeat_with_abort(~abort, ~f=() =>
    create(~message?, ~close_on_exec?, ~unlink_on_exit?, path)
  )
  >>| fail_on_abort(path, ~held_by=None);

let is_locked = path =>
  In_thread.run(() => Lock_file_blocking.is_locked(path));

module Nfs = {
  let get_hostname_and_pid = path =>
    In_thread.run(() => Lock_file_blocking.Nfs.get_hostname_and_pid(path));

  let get_message = path =>
    In_thread.run(() => Lock_file_blocking.Nfs.get_message(path));

  let unlock_exn = path =>
    In_thread.run(() => Lock_file_blocking.Nfs.unlock_exn(path));

  let unlock = path =>
    In_thread.run(() => Lock_file_blocking.Nfs.unlock(path));

  let create = (~message=?, path) =>
    In_thread.run(() => Lock_file_blocking.Nfs.create(~message?, path));

  let create_exn = (~message=?, path) =>
    In_thread.run(() => Lock_file_blocking.Nfs.create_exn(~message?, path));

  let waiting_create = (~abort=Deferred.never(), ~message=?, path) =>
    repeat_with_abort(~abort, ~f=() =>
      create(~message?, path)
      >>| (
        fun
        | Ok () => true
        | Error(_) => false
      )
    )
    >>| fail_on_abort(path, ~held_by=None);

  let critical_section = (~message=?, path, ~abort, ~f) =>
    waiting_create(~abort, ~message?, path)
    >>= (
      () =>
        Monitor.protect(~run=`Schedule, ~rest=`Log, f, ~finally=() =>
          unlock_exn(path)
        )
    );
};

module Flock = {
  type t = Lock_file_blocking.Flock.t;

  let lock_exn = (~lock_path) =>
    In_thread.run(() => Lock_file_blocking.Flock.lock_exn(~lock_path));

  let lock = (~lock_path) =>
    Monitor.try_with_or_error(~rest=`Log, ~extract_exn=true, () =>
      lock_exn(~lock_path)
    );

  let unlock_exn = t =>
    In_thread.run(() => Lock_file_blocking.Flock.unlock_exn(t));
  let unlock = t =>
    Monitor.try_with_or_error(~rest=`Log, ~extract_exn=true, () =>
      unlock_exn(t)
    );

  let wait_for_lock_exn = (~abort=Deferred.never(), ~lock_path, ()) => {
    let lock_handle = Set_once.create();
    let%map () =
      repeat_with_abort(~abort, ~f=() =>
        switch%map (lock_exn(~lock_path)) {
        | `We_took_it(t) =>
          Set_once.set_exn(lock_handle, [%here], t);
          true;
        | `Somebody_else_took_it => false
        }
      )
      >>| fail_on_abort(lock_path, ~held_by=None);

    Set_once.get_exn(lock_handle, [%here]);
  };

  let wait_for_lock = (~abort=?, ~lock_path, ()) =>
    Monitor.try_with_or_error(~rest=`Log, ~extract_exn=true, () =>
      wait_for_lock_exn(~abort?, ~lock_path, ())
    );
};

module Symlink = {
  type t = Lock_file_blocking.Symlink.t;

  let lock_exn = (~lock_path, ~metadata) =>
    In_thread.run(() =>
      Lock_file_blocking.Symlink.lock_exn(~lock_path, ~metadata)
    );

  let lock = (~lock_path, ~metadata) =>
    Monitor.try_with_or_error(~rest=`Log, ~extract_exn=true, () =>
      lock_exn(~lock_path, ~metadata)
    );

  let unlock_exn = t =>
    In_thread.run(() => Lock_file_blocking.Symlink.unlock_exn(t));
  let unlock = t =>
    Monitor.try_with_or_error(~rest=`Log, ~extract_exn=true, () =>
      unlock_exn(t)
    );

  let wait_for_lock_exn = (~abort=Deferred.never(), ~lock_path, ~metadata, ()) => {
    let lock_handle = Set_once.create();
    let last_lock_holder = ref(None);
    let%map () =
      repeat_with_abort(~abort, ~f=() =>
        switch%map (lock_exn(~lock_path, ~metadata)) {
        | `We_took_it(t) =>
          Set_once.set_exn(lock_handle, [%here], t);
          true;
        | `Somebody_else_took_it(other_party_info) =>
          last_lock_holder := Some(other_party_info);
          false;
        }
      )
      >>| fail_on_abort(
            lock_path,
            ~held_by=
              Option.map(
                last_lock_holder^,
                ~f=
                  fun
                  | Ok(s) => Sexp.Atom(s)
                  | Error(e) => [%sexp Error(e: Error.t)],
              ),
          );

    Set_once.get_exn(lock_handle, [%here]);
  };

  let wait_for_lock = (~abort=?, ~lock_path, ~metadata, ()) =>
    Monitor.try_with_or_error(~rest=`Log, ~extract_exn=true, () =>
      wait_for_lock_exn(~abort?, ~lock_path, ~metadata, ())
    );
};
