open! Core;
open Async;

let command = () =>
  Command.async_spec(
    ~summary="test async getaddrinfo",
    Command.Spec.(empty +> anon("HOSTNAME" %: string)),
    (host, ()) =>
    Unix.Addr_info.get(
      ~host,
      [
        Unix.Addr_info.AI_SOCKTYPE(Unix.SOCK_STREAM),
        Unix.Addr_info.AI_FAMILY(Unix.PF_INET),
        Unix.Addr_info.AI_CANONNAME,
      ],
    )
    >>= (
      addr_infos =>
        Deferred.List.iter(
          addr_infos,
          ~f=a => {
            Unix.Addr_info.sexp_of_t(a) |> Sexp.to_string |> print_endline;
            Unix.Name_info.get(a.Unix.Addr_info.ai_addr, [])
            >>| (
              name_info =>
                Unix.Name_info.sexp_of_t(name_info)
                |> Sexp.to_string
                |> print_endline
            );
          },
        )
    )
  );

let () = command() |> Command_unix.run;
