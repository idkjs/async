open Core;
open Async;
open Print;

/* ToDo: Come up with some instrumentation for heap size
 * as the program evolves.  Less important here since IVar
 * properties are well understood and not pathological. */
/* Uses deferred to structure backtracking search.  The
 * runtime queue represents the nodes that are waiting
 * to be visited.  Changing this behavior doesn't particularly
 * change behavior though because we demand all results. */
/* Should generate IVars equal to the branching factor plus
 * the depth of the search space [nq], and there should
 * only ever be one callback waiting on the result of a leaf
 * (since this is a search tree.) */
/* Hack: force IVar to be empty for some period of time. Can't
 * use higher level clock because if the time to wait is zero
 * it will immediately run without giving it to the scheduler. */
let ticky_return = x => Scheduler.schedule'(() => return(x));

let nqueens = nq => {
  let rec safe = (x, d) =>
    fun
    | [] => true
    | [q, ...l] => x != q && x != q + d && x != q - d && safe(x, d + 1, l);

  let nql = {
    /* not tail recursive */
    let rec f =
      fun
      | n when n == nq => [n]
      | n => [n, ...f(n + 1)];

    f(1);
  };

  let gen = bs =>
    List.concat(
      List.map(bs, ~f=b =>
        List.concat(
          List.map(nql, ~f=q =>
            if (safe(q, 1, b)) {
              [[q, ...b]];
            } else {
              [];
            }
          ),
        )
      ),
    );
  /* use option instead */

  let rec step = (n, b) =>
    if (n < nq) {
      Deferred.all(List.map(~f=step(n + 1), gen([b])))
      >>= (
        rs =>
          /* Should be Empty
             let r = ticky_return (List.concat rs)
             in print_endline (Sexp.to_string (Deferred.sexp_of_t (fun _ -> Sexp.Atom "...") r)); r */
          ticky_return(List.concat(rs))
      );
    } else {
      ticky_return([b]);
    };

  step(0, []);
};

let nq = 8;

let () =
  nqueens(nq)
  >>> (
    solns => {
      print_int(List.length(solns));
      print_newline();
      Shutdown.shutdown(0);
    }
  );

let () = never_returns(Scheduler.go());
