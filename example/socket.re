open Core;
open Async;
module Socket = Unix.Socket;

let printf = Print.printf;
let port = 10_000;

let doit = () => {
  let s =
    Socket.bind_inet(
      Socket.create(Socket.Type.tcp),
      Socket.Address.Inet.create_bind_any(~port),
    );

  don't_wait_for(
    switch%map (Socket.accept(Socket.listen(s))) {
    | `Socket_closed => ()
    | `Ok(s, _) =>
      let buf = Bytes.create(1000);
      let reader = Reader.create(Socket.fd(s));
      let rec loop = bytes_read =>
        upon(
          Reader.read(reader, buf),
          fun
          | `Eof => printf("EOF\n")
          | `Ok(n) => {
              let bytes_read = bytes_read + n;
              printf("read %d bytes in total.\n", bytes_read);
              loop(bytes_read);
            },
        );

      loop(0);
    },
  );
  upon(
    Clock.after(sec(2.)),
    () => {
      let s = Socket.create(Socket.Type.tcp);
      upon(
        Socket.connect(
          s,
          Socket.Address.Inet.create(
            Unix.Inet_addr.of_string("127.0.0.1"),
            ~port,
          ),
        ),
        s => {
          let w = Writer.create(Socket.fd(s));
          let buf = String.make(4096, ' ');
          let rec loop = bytes_written => {
            Writer.write(w, buf);
            upon(
              Writer.flushed(w),
              _ => {
                let bytes_written = bytes_written + String.length(buf);
                printf("wrote %d bytes in total.\n", bytes_written);
                loop(bytes_written);
              },
            );
          };

          loop(0);
        },
      );
    },
  );
};

let () = doit();
let () = never_returns(Scheduler.go());
