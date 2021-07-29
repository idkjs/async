open Core;
open Async;
open Rpc;

/* Generic wrapper over both the Async.Tcp and Netkit backed RPC implementations */

module Server: {
  type t;

  let bound_on: t => int;
  let close: t => Deferred.t(unit);
};

type t;

let make_client:
  (~heartbeat_config: Connection.Heartbeat_config.t=?, t, string, int) =>
  Deferred.t(result(Connection.t, Exn.t));

let with_client:
  (
    ~heartbeat_config: Connection.Heartbeat_config.t=?,
    t,
    string,
    int,
    Connection.t => Deferred.t('a)
  ) =>
  Deferred.t(result('a, Exn.t));

let make_server:
  (
    ~heartbeat_config: Connection.Heartbeat_config.t=?,
    ~port: int=?,
    ~implementations: Implementations.t('a),
    ~initial_connection_state: Connection.t => 'a,
    t
  ) =>
  Deferred.t(Server.t);

let spec:
  unit =>
  Command.Spec.t(
    (~rpc_impl: t, unit) => Deferred.t(unit),
    unit => Deferred.t(unit),
  );
