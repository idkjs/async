/** [Unpack_sequence] uses an [Unpack_buffer.t] to unpack a sequence of packed values
    coming from a [string Pipe.Reader.t] or a [Reader.t].  It can produce a pipe of
    unpacked values or iterate a user-supplied function over the unpacked values. */;

open! Core;
open! Async;
open! Import;

module Unpack_iter_result: {
  [@deriving sexp_of]
  type t('a) =
    | Input_closed
    | Input_closed_in_the_middle_of_data(Unpack_buffer.t('a))
    | Unpack_error(Error.t);

  let to_error: t(_) => Error.t;
};

module Unpack_result: {
  [@deriving sexp_of]
  type t('a) =
    | Input_closed
    | Input_closed_in_the_middle_of_data(Unpack_buffer.t('a))
    | Output_closed
    | Unpack_error(Error.t);

  let to_error: t(_) => Error.t;
};

/** [Unpack_from] specifies the source of the sequence of bytes to unpack from. */

module Unpack_from: {
  type t =
    | Pipe(Pipe.Reader.t(string))
    | Reader(Reader.t);
};

/** [unpack_into_pipe ~from:input ~using:unpack_buffer] returns [(output, result)], and
    uses [unpack_buffer] to unpack values from [input] until [input] is closed.  It puts
    the unpacked values into [output], which is closed once unpacking finishes, be it
    normally or due to an error.  [result] indicates why unpacking finished.

    To unpack from a [bin_reader], use:

    {[
      unpack_into_pipe ~from ~using:(Unpack_buffer.create_bin_prot bin_reader)
    ]}

    Using [~from:(Reader reader)] is more efficient than [~from:(Pipe (Reader.pipe
    reader))] because it blits bytes directly from the reader buffer to the unpack buffer,
    without any intervening allocation. */

let unpack_into_pipe:
  (~from: Unpack_from.t, ~using: Unpack_buffer.t('a)) =>
  (Pipe.Reader.t('a), Deferred.t(Unpack_result.t('a)));

/** [unpack_iter] is a more efficient version of [unpack_into_pipe] that calls [f] on each
    value as it is unpacked, rather than putting the value into a pipe.  If [f] raises,
    then the result will be [Unpack_error]. */

let unpack_iter:
  (~from: Unpack_from.t, ~using: Unpack_buffer.t('a), ~f: 'a => unit) =>
  Deferred.t(Unpack_iter_result.t('a));
