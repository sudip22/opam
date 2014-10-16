(**************************************************************************)
(*                                                                        *)
(*    Copyright 2012-2013 OCamlPro                                        *)
(*    Copyright 2012 INRIA                                                *)
(*                                                                        *)
(*  All rights reserved.This file is distributed under the terms of the   *)
(*  GNU Lesser General Public License version 3.0 with linking            *)
(*  exception.                                                            *)
(*                                                                        *)
(*  OPAM is distributed in the hope that it will be useful, but WITHOUT   *)
(*  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY    *)
(*  or FITNESS FOR A PARTICULAR PURPOSE.See the GNU General Public        *)
(*  License for more details.                                             *)
(*                                                                        *)
(**************************************************************************)

open OpamMisc.OP

let log fmt = OpamGlobals.log "PARALLEL" fmt
let slog = OpamGlobals.slog

module type G = sig
  include Graph.Sig.I
  include Graph.Topological.G with type t := t and module V := V
  val has_cycle: t -> bool
  val scc_list: t -> V.t list list
  val string_of_vertex: V.t -> string
end

type error =
  | Process_error of OpamProcess.result
  | Internal_error of string
  | Package_error of string

module type SIG = sig

  module G : G

  type ('a,'b) seq_command = Done of 'a
                           | Run of 'b * (string * string list)

  val iter:
    jobs:int ->
    command:(pred:(G.V.t * 'a) list -> G.V.t -> ('a,'b) seq_command) ->
    post_command:('b -> OpamProcess.result -> ('a,'b) seq_command) ->
    G.t ->
    unit
(*
  val iter: int -> G.t ->
    pre:(G.V.t -> unit) ->
    child:(G.V.t -> unit) ->
    post:(G.V.t -> unit) ->
    unit
*)
  val iter_l: int -> G.vertex list ->
    pre:(G.V.t -> unit) ->
    child:(G.V.t -> unit) ->
    post:(G.V.t -> unit) ->
    unit

  val map_reduce: int -> G.t ->
    map:(G.V.t -> 'a) ->
    merge:('a -> 'a -> 'a) ->
    init:'a ->
    'a

  val map_reduce_l: int -> G.vertex list ->
    map:(G.V.t -> 'a) ->
    merge:('a -> 'a -> 'a) ->
    init:'a ->
    'a

  val create: G.V.t list -> G.t

  exception Errors of (G.V.t * error) list * G.V.t list
  exception Cyclic of G.V.t list list
end

module Make (G : G) : SIG with module G = G
                           and type G.V.t = G.V.t
= struct

  module G = G

  module V = G.V (* struct include G.V let compare = compare end *)
  module M = Map.Make (V)
  module S = Set.Make (V)

  type t = {
    graph  : G.t ;      (* The original graph *)
    visited: S.t ;      (* The visited nodes *)
    roots  : S.t ;      (* The current roots *)
    degree : int M.t ;  (* Node degrees *)
  }

  let init graph =
    let roots =
      G.fold_vertex
        (fun v (todo,degree) ->
          let d = G.in_degree graph v in
          if d = 0 then
            S.add v todo, degree
          else
            todo, M.add v d degree
        ) graph (S.empty,M.empty) in
    { graph ; roots ; degree ; visited = S.empty }

  let visit t x =
    if S.mem x t.visited then
      invalid_arg "This node has already been visited.";
    if not (S.mem x t.roots) then
      invalid_arg "This node is not a root node";
    (* Add the node to the list of visited nodes *)
    let t = { t with visited = S.add x t.visited } in
    (* Remove the node from the list of root nodes *)
    let roots = S.remove x t.roots in
    let degree = ref t.degree in
    let remove_degree x = degree := M.remove x !degree in
    let replace_degree x d = degree := M.add x d (M.remove x !degree) in
    (* Update the children of the node by decreasing by 1 their in-degree *)
    let roots =
      G.fold_succ
        (fun x l ->
          let d = M.find x t.degree in
          if d = 1 then (
            remove_degree x;
            S.add x l
          ) else (
            replace_degree x (d-1);
            l
          )
        ) t.graph x roots in
    { t with roots; degree = !degree }

  (* the [Unix.wait] might return a processus which has not been created
     by [Unix.fork]. [wait pids] waits until a process in [pids]
     terminates. *)
  (* XXX: this will not work under windows *)
  let string_of_pids pids =
    Printf.sprintf "{%s}"
      (String.concat ","
         (OpamMisc.IntMap.fold (fun e _ l -> string_of_int e :: l) pids []))

  let string_of_status st =
    let st, n = match st with
      | Unix.WEXITED n -> "exit", n
      | Unix.WSIGNALED n -> "signal", n
      | Unix.WSTOPPED n -> "stop", n in
    Printf.sprintf "%s %d" st n

  let wait pids =
    let rec aux () =
      let pid, status = Unix.wait () in
      if OpamMisc.IntMap.mem pid pids then (
        log "%d is dead (%a)" pid (slog string_of_status) status;
        pid, status
      ) else (
        log "%d: unknown child (pids=%a)!"
          pid
          (slog string_of_pids) pids;
        aux ()
      ) in
    aux ()

  exception Errors of (G.V.t * error) list * G.V.t list
  exception Cyclic of G.V.t list list

  open S.Op
(*
  (* write and close the output channel *)
  let write_error oc r =
    log "write_error";
    Marshal.to_channel oc r [];
    close_out oc

  (* read and close the input channel *)
  let read_error ic =
    log "read_error";
    let r : error =
      try Marshal.from_channel ic
      with e -> (* Marshal doesn't document its exceptions :( *)
        OpamMisc.fatal e;
        Internal_error "Cannot read the error file" in
    close_in ic;
    r
*)
  let map ~jobs ~command ~post_command g =
    let t = ref (init g) in
    (* node -> OpamProcess.t * 'a *)
    let running = ref M.empty in
    (* The nodes to process *)
    let todo = ref (!t.roots) in
    (* The computed results *)
    let results = ref M.empty in

    (* All the node with a current worker currently doing some processing. *)
    let worker_nodes () = S.of_list (M.keys !running) in

    log "Iterate over %a task(s) with %d process(es)"
      (slog @@ G.nb_vertex @> string_of_int) g n;

    if G.has_cycle !t.graph then (
      let sccs = G.scc_list !t.graph in
      let sccs = List.filter (function _::_::_ -> true | _ -> false) sccs in
      raise (Cyclic sccs)
    );

    let get_errors errors =
      (* Generate the remaining nodes in topological order *)
      let remaining =
        G.fold_vertex (fun v l ->
            if S.mem v !t.visited || M.mem v errors then l
            else v::l
          ) !t.graph [] in
      Errors (M.bindings errors, List.rev remaining) in

    (* nslots is the number of free slots *)
    let rec loop nslots running todo results =

      let run_seq_command todo n = function
        | Done r ->
          loop (nslots + 1) (M.remove n running) todo (M.add n r results)
        | Run (x,(cmd,args)) ->
          let p = OpamProcess.run_background cmd args in
          loop nslots (M.add n (p,x) !running) todo results
      in

      if M.is_empty running && S.is_empty !t.roots then ()
      else if nslots <= 0 || M.cardinal running = S.cardinal !t.roots then (

        (* if either 1/ no slots are available or 2/ no action can be
           performed, then wait for a child process to finish its work *)
        log "waiting for a child process to finish";
        let processes = M.fold (fun n (p,x) acc -> p,(n,x)) !running [] in
        let process,result =
          let processes = List.map fst processes in
          try OpamProcess.wait_one processes
          with e ->
            OpamGlobals.error "%s" (Printexc.to_string e);
            (* Cleanup *)
            let errors =
              List.fold_left (fun p ->
                  M.add n (Internal_error "User interruption") errors;
                  try Unix.kill p.OpamProcess.p_pid Sys.sigint with _ -> ()
                ) errors processes;
            (try
               List.iter (fun _ _ -> ignore (OpamProcess.wait_one processes))
                 processes
             with e -> log "%a in sub-process cleanup"
                         (slog Printexc.to_string) e);
            raise (get_errors ())
        in
        let n,x = List.assoc process processes in
        run_seq_command (post_command x result)
      ) else (

        (* otherwise, if the todo list is empty, then refill it *)
        if S.is_empty !todo then (
          log "refilling the TODO list";
          todo := !t.roots -- worker_nodes () -- error_nodes ();
        );

        (* finally, if the todo list contains at least a node action,
           then simply process it *)
        let n = S.choose !todo in
        todo := S.remove n !todo;

        (* (\* Set-up a channel from the child to the parent *\) *)
        (* let error_file = OpamSystem.temp_file "error" in *)

        (* We execute the 'pre' function before the fork *)
        let pred = G.pred g n in
        let pred = List.map (fun n -> n, M.assoc n !results) in
        run_seq_command (command ~pred n)
      ) in
    loop jobs;
    

  let map_reduce jobs g ~map ~merge ~init =
    let files = ref [] in
    let file repo = List.assoc repo !files in

    let pre repo =
      let tmpfile = OpamSystem.temp_file "map-reduce" in
      log "pre %a (%s)" (slog G.string_of_vertex) repo tmpfile;
      files := (repo, tmpfile) :: !files
    in

    let child repo =
      log "child %a" (slog G.string_of_vertex) repo;
      let file = file repo in
      let result = map repo in
      let oc = open_out file in
      Marshal.to_channel oc result []
    in

    let acc = ref init in
    let post repo =
      log "post %a" (slog G.string_of_vertex) repo;
      let file = file repo in
      let ic = open_in_bin file in
      let result =
        try Marshal.from_channel ic
        with e ->
          OpamMisc.fatal e;
          OpamSystem.internal_error "Cannot read the result file" in
      close_in ic;
      Unix.unlink file;
      files := List.filter (fun (_,f) -> f<>file) !files;
      acc := merge result !acc
    in

    try
      ignore (iter jobs g ~pre ~child ~post);
      !acc, 0
    with
    | Errors (errors,_) ->
      let string_of_error = function
        | Process_error r  -> OpamProcess.string_of_result r
        | Package_error s -> s
        | Internal_error s -> s in
      List.iter (fun (v, e) ->
        OpamGlobals.error "While processing %s:\n%s"
          (G.string_of_vertex v)
          (string_of_error e);
      ) errors;
      OpamGlobals.exit 2

  let create l =
    let g = G.create () in
    List.iter (G.add_vertex g) l;
    g

  let map_reduce_l jobs list ~map ~merge ~init = match list with
    | []    -> init, 0
    | [elt] -> merge (map elt) init, 0
    | _     ->
      if jobs = 1 then
        List.fold_left (fun acc repo -> merge (map repo) acc) init list, 0
      else
        let g = create list in
        map_reduce jobs g ~map ~merge ~init

  let iter_l jobs list ~pre ~child ~post = match list with
    | []    -> 0
    | [elt] -> pre elt; child elt; post elt; 0
    | list  ->
      if jobs = 1 then
        (List.iter (fun elt -> pre elt; child elt; post elt) list; 0)
      else
        let g = create list in
        iter jobs g ~pre ~post ~child; 0

end

module type VERTEX = sig
  include Graph.Sig.COMPARABLE
  val to_string: t -> string
end

module type GRAPH = sig
  include Graph.Sig.I
  include Graph.Oper.S with type g = t
  module Topological : sig
    val fold : (V.t -> 'a -> 'a) -> t -> 'a -> 'a
    val iter : (V.t -> unit) -> t -> unit
  end
  module Parallel : SIG with type G.t = t
                         and type G.V.t = vertex
  module Dot : sig val output_graph : out_channel -> t -> unit end
end

module MakeGraph (V: VERTEX) : GRAPH with type V.t = V.t
= struct
  module PG = Graph.Imperative.Digraph.ConcreteBidirectional (V)
  module Topological = Graph.Topological.Make (PG)
  module Traverse = Graph.Traverse.Dfs(PG)
  module Components = Graph.Components.Make(PG)
  module Parallel = Make (struct
      let string_of_vertex = V.to_string
      include PG
      include Topological
      include Traverse
      include Components
    end)
  module Dot = Graph.Graphviz.Dot (struct
      let edge_attributes _ = []
      let default_edge_attributes _ = []
      let get_subgraph _ = None
      let vertex_attributes _ = []
      let vertex_name v = Printf.sprintf "%S" (V.to_string v)
      let default_vertex_attributes _ = []
      let graph_attributes _ = []
      include PG
    end)
  include PG
  include Graph.Oper.I (PG)
end
