open! Core;
open! Import;

/*_ The [Reader] and [Writer] modules from [Async], renamed to avoid conflicting with
  [Reader] and [Writer] below. */
module Async_reader := Reader;
module Async_writer := Writer;

module Reader: {
  include (module type of {
    include Rpc_kernel.Transport.Reader;
  });

  let of_reader: (~max_message_size: int=?, Async_reader.t) => t;
};

module Writer: {
  include (module type of {
    include Rpc_kernel.Transport.Writer;
  });

  let of_writer: (~max_message_size: int=?, Async_writer.t) => t;
};

include

     (module type of {
      include Rpc_kernel.Transport;
    }) with
      module Reader := Rpc_kernel.Transport.Reader with
    module Writer := Rpc_kernel.Transport.Writer;

let of_reader_writer:
  (~max_message_size: int=?, Async_reader.t, Async_writer.t) => t;

let of_fd:
  (
    ~buffer_age_limit: Async_writer.buffer_age_limit=?,
    ~reader_buffer_size: int=?,
    ~max_message_size: int,
    Fd.t
  ) =>
  t;

module Tcp: {
  type transport_maker :=
    (Fd.t, ~max_message_size: int) => Rpc_kernel.Transport.t;

  /** [serve] takes a callback; your callback will be handed a [Rpc.Transport.t] and it's
      your responsibility to create the [Rpc.Connection.t]. The transport will be closed
      when your callback returns; ending your callback with a call to
      [Rpc.Connection.close_finished] is likely appropriate. */

  let serve:
    (
      ~where_to_listen: Tcp.Where_to_listen.t('address, 'listening_on),
      ~max_connections: int=?,
      ~backlog: int=?,
      ~drop_incoming_connections: bool=?,
      ~time_source: Time_source.T1.t([> read])=?,
      ~max_message_size: int=?,
      ~make_transport: /** default is [of_fd] (as opposed to [Rpc_transport_low_latency]) */ transport_maker
                         =?,
      ~auth: 'address => bool=?,
      ~on_handler_error: /** default is [`Ignore] */ [
                           | `Raise
                           | `Ignore
                           | `Call(('address, exn) => unit)
                         ]
                           =?,
      (
        ~client_addr: 'address,
        ~server_addr: 'address,
        Rpc_kernel.Transport.t
      ) =>
      Deferred.t(unit)
    ) =>
    Deferred.t(Tcp.Server.t('address, 'listening_on));

  /** [serve_inet] is like [serve] but only for inet sockets (not unix sockets), and
      returning the server immediately, without deferreds. */

  let serve_inet:
    (
      ~where_to_listen: Tcp.Where_to_listen.t(Socket.Address.Inet.t, int),
      ~max_connections: int=?,
      ~backlog: int=?,
      ~drop_incoming_connections: bool=?,
      ~time_source: Time_source.T1.t([> read])=?,
      ~max_message_size: int=?,
      ~make_transport: transport_maker=?,
      ~auth: Socket.Address.Inet.t => bool=?,
      ~on_handler_error: [
                           | `Raise
                           | `Ignore
                           | `Call((Socket.Address.Inet.t, exn) => unit)
                         ]
                           =?,
      (
        ~client_addr: Socket.Address.Inet.t,
        ~server_addr: Socket.Address.Inet.t,
        Rpc_kernel.Transport.t
      ) =>
      Deferred.t(unit)
    ) =>
    Tcp.Server.t(Socket.Address.Inet.t, int);

  /** [connect ?make_transport where_to_connect ()] connects to the server at
      [where_to_connect]. On success, it returns the transport created using
      [make_transport] and the [Socket.Address.t] that it connected to, otherwise it
      returns the Error.

      It is your responsibility to close the [Transport.t] */

  let connect:
    (
      ~max_message_size: int=?,
      ~make_transport: /** default is [of_fd] (as opposed to [Rpc_transport_low_latency]) */ transport_maker
                         =?,
      ~tcp_connect_timeout: Time_ns.Span.t=?,
      Tcp.Where_to_connect.t('addr)
    ) =>
    Deferred.t(Result.t((Rpc_kernel.Transport.t, 'addr), Exn.t));
};
