open Core;
open Async;

/* The list of implementations supported by the server.  The server state is simply a
   counter used for allocating unique ids. */
let implementations = [
  Rpc.Rpc.implement(
    Rpc_intf.get_unique_id,
    (ctr, ()) => {
      printf(".%!");
      incr(ctr);
      return(ctr^);
    },
  ),
  Rpc.Rpc.implement(
    Rpc_intf.set_id_counter,
    (ctr, i) => {
      printf("!%!");
      if (i == 0) {
        failwith("Can't set counter back to zero");
      };
      return(ctr := i);
    },
  ),
  Rpc.Pipe_rpc.implement(
    Rpc_intf.counter_values,
    (ctr, ()) => {
      let (r, w) = Pipe.create();
      let last_value = ref(ctr^);
      let send = () => {
        last_value := ctr^;
        Pipe.write(w, ctr^);
      };

      don't_wait_for(send());
      Clock.every'(~stop=Pipe.closed(w), sec(0.1), () =>
        if (last_value^ != ctr^) {
          send();
        } else {
          return();
        }
      );
      return(Ok(r));
    },
  ),
];

let main = (~port) => {
  let counter = ref(0);
  let implementations =
    Rpc.Implementations.create(
      ~implementations,
      ~on_unknown_rpc=`Close_connection,
    );

  switch (implementations) {
  | Error(`Duplicate_implementations(_: list(Rpc.Description.t))) =>
    assert(false)
  | Ok(implementations) =>
    let server =
      Tcp.Server.create(
        Tcp.Where_to_listen.of_port(port),
        ~on_handler_error=`Ignore,
        (_: Socket.Address.Inet.t, reader, writer) =>
        Rpc.Connection.server_with_close(
          reader,
          writer,
          ~implementations,
          ~connection_state=(_: Rpc.Connection.t) => counter,
          ~on_handshake_error=`Ignore,
        )
      );

    ignore(server: Deferred.t(Tcp.Server.t(Socket.Address.Inet.t, int)));
    never();
  };
};

let () =
  Command.async(
    ~summary="A trivial Async-RPC server",
    {
      let%map_open.Command port =
        flag(
          "-port",
          ~doc=" Port to listen on",
          optional_with_default(8080, int),
        );

      () => main(~port);
    },
  )
  |> Command_unix.run;
