open Core;
open Poly;
open Async;
open Deferred.Let_syntax;
module P = Async_rpc_kernel.Async_rpc_kernel_private.Protocol;
module T = Rpc.Transport;

let copy = (conn_number, desc, reader, writer) => {
  let n = ref(0);
  T.Reader.read_forever(
    reader,
    ~on_message=
      (buf, ~pos, ~len) => {
        let num = n^;
        incr(n);
        let pos_ref = ref(pos);
        let item =
          if (num == 0) {
            let header = P.Header.bin_read_t(buf, ~pos_ref);
            assert(pos_ref^ == pos + len);
            assert(
              T.Writer.send_bin_prot(writer, P.Header.bin_writer_t, header)
              == Sent(),
            );
            %sexp
            Header(header: P.Header.t);
          } else {
            let msg = P.Message.bin_read_nat0_t(buf, ~pos_ref);
            let left = len - (pos_ref^ - pos);
            assert(left >= 0);
            assert(
              T.Writer.send_bin_prot_and_bigstring(
                writer,
                P.Message.bin_writer_nat0_t,
                msg,
                ~buf,
                ~pos=pos_ref^,
                ~len=left,
              )
              == Sent(),
            );
            let sexp_of_data = len => {
              let len = (len: Bin_prot.Nat0.t :> int);
              let disp_len = min(16, len);
              Sexp.List(
                [
                  Sexp.Atom(sprintf("len=%d", len)),
                  ...List.init(
                       disp_len,
                       ~f=i => {
                         let x = Char.to_int(buf.{pos_ref^ + i});
                         Sexp.Atom(sprintf("%02x", x));
                       },
                     ),
                ]
                @ (
                  if (len > disp_len) {
                    [Atom("...")];
                  } else {
                    [];
                  }
                ),
              );
            };

            %sexp
            (msg: P.Message.t(data));
          };

        printf(
          "%{sexp:Sexp.t}\n"^,
          [%sexp (conn_number: int, desc: string, item: Sexp.t)],
        );
        Continue;
      },
    ~on_end_of_batch=ignore,
  )
  >>| ignore;
};

let main = (~host, ~lport, ~dport) => {
  let n = ref(0);
  let%bind _ =
    Tcp.Server.create_sock(
      Tcp.Where_to_listen.of_port(lport),
      ~on_handler_error=`Raise,
      (_inet, s1) => {
        let num = n^;
        incr(n);
        let%bind s2 =
          Tcp.connect_sock(
            Tcp.Where_to_connect.of_host_and_port({host, port: dport}),
          );

        let t1 = T.of_fd(Socket.fd(s1), ~max_message_size=1 lsl 30);
        let t2 = T.of_fd(Socket.fd(s2), ~max_message_size=1 lsl 30);
        Deferred.all_unit([
          copy(num, "client->server", t1.reader, t2.writer),
          copy(num, "server->client", t2.reader, t1.writer),
        ]);
      },
    );

  Deferred.never();
};

let param = {
  open Command.Let_syntax;
  let%map_open host =
    flag(
      "-host",
      optional_with_default("127.0.0.1", string),
      ~doc=" host to connect to",
    )
  and lport = flag("-lport", required(int), ~doc=" port to serve on")
  and dport = flag("-dport", required(int), ~doc=" port to connect to");
  () => main(~host, ~lport, ~dport);
};

let command =
  Command.async(
    param,
    ~summary=
      "Small Async RPC proxy to inspect the traffic of an async RPC connection",
  );
