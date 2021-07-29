open Core;
open Async;

module Hello = {
  module Model = {
    let name = "hello";

    type query = unit;
    type response = unit;
  };

  include Model;
  include Versioned_rpc.Caller_converts.Rpc.Make(Model);

  module V1 =
    Register({
      let version = 1;

      [@deriving bin_io]
      type query = unit;
      [@deriving bin_io]
      type response = unit;

      let query_of_model = Fn.id;
      let model_of_response = Fn.id;
    });
};

let%expect_test _ = {
  let implementations =
    Rpc.Implementations.create_exn(
      ~on_unknown_rpc=`Close_connection,
      ~implementations=
        Versioned_rpc.Menu.add([
          Rpc.Rpc.implement(
            Hello.V1.rpc,
            ((), ()) => {
              printf("server says hi\n");
              return();
            },
          ),
        ]),
    );

  let%bind server =
    Rpc.Connection.serve(
      ~implementations,
      ~initial_connection_state=(_addr, _conn) => (),
      ~where_to_listen=Tcp.Where_to_listen.of_port_chosen_by_os,
      (),
    );

  let host_and_port =
    Host_and_port.create(
      ~host="localhost",
      ~port=Tcp.Server.listening_on(server),
    );

  /* test Persistent_connection.Rpc */
  let on_unversioned_event:
    Persistent_connection.Rpc.Event.t => Deferred.t(unit) = (
    fun
    | Obtained_address(_) => {
        printf("(Obtained_address <elided>)\n");
        return();
      }
    | event => {
        print_s([%sexp (event: Persistent_connection.Rpc.Event.t)]);
        return();
      }:
      Persistent_connection.Rpc.Event.t => Deferred.t(unit)
  );

  let unversioned_conn =
    Persistent_connection.Rpc.create'(
      ~on_event=on_unversioned_event, ~server_name="unversioned rpc", () =>
      return(Ok(host_and_port))
    );

  %expect
  {| Attempting_to_connect |};
  let%bind this_conn = Persistent_connection.Rpc.connected(unversioned_conn);
  %expect
  {|
        (Obtained_address <elided>)
        (Connected <opaque>)
      |};
  let%bind () = Rpc.Rpc.dispatch_exn(Hello.V1.rpc, this_conn, ());
  %expect
  {| server says hi |};
  let%bind () = Persistent_connection.Rpc.close(unversioned_conn);
  %expect
  {| Disconnected |};
  /* test Persistent_connection.Versioned_rpc */
  let on_versioned_event:
    Persistent_connection.Versioned_rpc.Event.t => Deferred.t(unit) = (
    fun
    | Obtained_address(_) => {
        printf("(Obtained_address <elided>)\n");
        return();
      }
    | event => {
        print_s([%sexp (event: Persistent_connection.Versioned_rpc.Event.t)]);
        return();
      }:
      Persistent_connection.Versioned_rpc.Event.t => Deferred.t(unit)
  );

  let versioned_conn =
    Persistent_connection.Versioned_rpc.create'(
      ~on_event=on_versioned_event, ~server_name="versioned rpc", () =>
      return(Ok(host_and_port))
    );

  %expect
  {| Attempting_to_connect |};
  let%bind this_conn =
    Persistent_connection.Versioned_rpc.connected(versioned_conn);
  %expect
  {|
        (Obtained_address <elided>)
        (Connected <opaque>)
      |};
  let%bind () =
    Hello.dispatch_multi(this_conn, ()) |> Deferred.Or_error.ok_exn;
  %expect
  {| server says hi |};
  let%bind () = Persistent_connection.Versioned_rpc.close(versioned_conn);
  %expect
  {|
    Disconnected |};
  return();
};
