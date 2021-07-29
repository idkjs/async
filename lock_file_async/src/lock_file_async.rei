/** [Lock_file_async] is a wrapper that provides Async equivalents for
    {{!Lock_file_blocking}[Lock_file_blocking]}. */;

open! Core;
open! Async;
open! Import;

/** [create ?message path] tries to create a file at [path] containing the text [message],
    pid if none provided.  It returns true on success, false on failure.  Note: there is
    no way to release the lock or the fd created inside!  It will only be released when
    the process dies.*/

let create:
  (
    ~message: string=?,
    ~close_on_exec: /** default is [true] */ bool=?,
    ~unlink_on_exit: /** default is [false] */ bool=?,
    string
  ) =>
  Deferred.t(bool);

/** [create_exn ?message path] is like [create] except that it throws an exception on
    failure instead of returning a boolean value. */

let create_exn:
  (
    ~message: string=?,
    ~close_on_exec: /** default is [true] */ bool=?,
    ~unlink_on_exit: /** default is [false] */ bool=?,
    string
  ) =>
  Deferred.t(unit);

/** [waiting_create path] repeatedly tries to lock [path], becoming determined when [path]
    is locked or raising when [abort] becomes determined.  Similar to
    {!Lock_file_blocking.create}. */

let waiting_create:
  (
    ~abort: /** default is [Deferred.never ()] */ Deferred.t(unit)=?,
    ~message: string=?,
    ~close_on_exec: /** default is [true] */ bool=?,
    ~unlink_on_exit: /** default is [false] */ bool=?,
    string
  ) =>
  Deferred.t(unit);

/** [is_locked path] returns true when the file at [path] exists and is locked, false
    otherwise. */

let is_locked: string => Deferred.t(bool);

/** [Nfs] has analogs of functions in {!Lock_file_blocking.Nfs}; see
    there for documentation.  In addition to adding [Deferred]'s, [blocking_create] was
    renamed [waiting_create] to avoid the impression that it blocks Async. */

module Nfs: {
  let create: (~message: string=?, string) => Deferred.Or_error.t(unit);
  let create_exn: (~message: string=?, string) => Deferred.t(unit);

  let waiting_create:
    (
      ~abort: /** default is [Deferred.never ()]. */ Deferred.t(unit)=?,
      ~message: string=?,
      string
    ) =>
    Deferred.t(unit);

  let unlock_exn: string => Deferred.t(unit);
  let unlock: string => Deferred.Or_error.t(unit);

  let critical_section:
    (
      ~message: string=?,
      string,
      ~abort: Deferred.t(unit),
      ~f: unit => Deferred.t('a)
    ) =>
    Deferred.t('a);

  let get_hostname_and_pid: string => Deferred.t(option((string, Pid.t)));
  let get_message: string => Deferred.t(option(string));
};

module Flock: {
  /** [Flock] has async analogues of functions in
      {{!Lock_file_blocking.Flock}[Lock_file_blocking.Flock]}; see there for
      documentation.

      Additionally, here we:

      - catch unix exceptions, packaging them as [Deferred.Or_error.t]
      - implement abortable waiting versions based on polling */

  type t;

  let lock_exn:
    (~lock_path: string) =>
    Deferred.t([ | `Somebody_else_took_it | `We_took_it(t)]);

  let lock:
    (~lock_path: string) =>
    Deferred.Or_error.t([ | `Somebody_else_took_it | `We_took_it(t)]);

  let unlock_exn: t => Deferred.t(unit);
  let unlock: t => Deferred.Or_error.t(unit);

  /** [wait_for_lock_exn ?abort ~lock_path ()] Wait for the lock, giving up once [abort]
      becomes determined */

  let wait_for_lock_exn:
    (
      ~abort: /** default is [Deferred.never ()] */ Deferred.t(unit)=?,
      ~lock_path: string,
      unit
    ) =>
    Deferred.t(t);

  /** See [wait_for_lock_exn] */

  let wait_for_lock:
    (~abort: Deferred.t(unit)=?, ~lock_path: string, unit) =>
    Deferred.Or_error.t(t);
};

module Symlink: {
  /** [Symlink] has async analogues of functions in
      {{!Lock_file_blocking.Symlink}[Lock_file_blocking.Symlink]}; see there for
      documentation.

      Additionally, here we:

      - catch unix exceptions, packaging them as [Deferred.Or_error.t]
      - implement abortable waiting versions based on polling */

  type t;

  let lock_exn:
    (~lock_path: string, ~metadata: string) =>
    Deferred.t(
      [ | `Somebody_else_took_it(Or_error.t(string)) | `We_took_it(t)],
    );

  let lock:
    (~lock_path: string, ~metadata: string) =>
    Deferred.Or_error.t(
      [ | `Somebody_else_took_it(Or_error.t(string)) | `We_took_it(t)],
    );

  let unlock_exn: t => Deferred.t(unit);
  let unlock: t => Deferred.Or_error.t(unit);

  /** [wait_for_lock_exn ?abort ~lock_path ()] Wait for the lock, giving up once [abort]
      becomes determined */

  let wait_for_lock_exn:
    (
      ~abort: /** default is [Deferred.never ()] */ Deferred.t(unit)=?,
      ~lock_path: string,
      ~metadata: string,
      unit
    ) =>
    Deferred.t(t);

  /** See [wait_for_lock_exn] */

  let wait_for_lock:
    (
      ~abort: Deferred.t(unit)=?,
      ~lock_path: string,
      ~metadata: string,
      unit
    ) =>
    Deferred.Or_error.t(t);
};
