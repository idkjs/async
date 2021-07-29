open! Core;
open! Async;

/** Query for grabbing a unique ID */

let get_unique_id: Rpc.Rpc.t(unit, int);

/** Query for setting the counter used to generate unique IDs */

let set_id_counter: Rpc.Rpc.t(int, unit);

/** This is a deprecated query, no longer supported by the server */

let set_id_counter_v0: Rpc.Rpc.t((int, int), unit);

/** For getting a stream updating the counter values */

let counter_values: Rpc.Pipe_rpc.t(unit, int, unit);
