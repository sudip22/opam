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

module type VERTEX = sig
  include OpamMisc.OrderedType
  include Graph.Sig.COMPARABLE with type t := t
end

module type G = sig
  include Graph.Sig.I
  module Vertex: VERTEX with type t = V.t
  module Topological: sig
    val fold: (V.t -> 'a -> 'a) -> t -> 'a -> 'a
  end
  val has_cycle: t -> bool
  val scc_list: t -> V.t list list
end

type command
val command:
  ?env:string array -> ?verbose:bool -> ?name:string ->
  ?metadata:(string*string) list -> ?dir:string ->
  ?text:string ->
  string -> string list -> command

val string_of_command: command -> string

type 'a job =
  | Done of 'a
  | Run of command * (OpamProcess.result -> 'a job)

(** Simply parallel execution of tasks *)

val iter: jobs:int -> command:('a -> unit job) -> 'a list -> unit

val map: jobs:int -> command:('a -> 'b job) -> 'a list -> 'b list

val reduce: jobs:int -> command:('a -> 'b job) ->
  merge:('b -> 'b -> 'b) -> nil:'b ->
  'a list -> 'b

(** More complex parallelism with dependency graphs *)

module type SIG = sig

  module G : G

  val iter:
    jobs:int ->
    command:(pred:(G.V.t * 'a) list -> G.V.t -> 'a job) ->
    G.t ->
    unit

  exception Errors of (G.V.t * exn) list * G.V.t list
  exception Cyclic of G.V.t list list
end

module Make (G : G) : SIG with module G = G
                           and type G.V.t = G.V.t

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

module MakeGraph (V: OpamMisc.OrderedType) : GRAPH with type V.t = V.t

(** Helper module to handle job-returning functions *)
module Job: sig
  type 'a t = 'a job =
      | Done of 'a
      | Run of command * (OpamProcess.result -> 'a job)

  module Op: sig
    (** This module makes construction of job-returning functions easier when
        open *)
    type 'a job = 'a t =
      | Done of 'a
      | Run of command * (OpamProcess.result -> 'a job)

    (** Stage a shell command with its continuation, eg:
        {[
          command "ls" ["-a"] @@> fun result ->
          if OpamProcess.is_success result then Done result.r_stdout
          else failwith "ls"
        ]}
    *)
    val (@@>): command -> (OpamProcess.result -> 'a job) -> 'a job

    (** [job1 @@+ fun r -> job2] appends the computation of tasks in [job2] after
        [job1] *)
    val (@@+): 'a job -> ('a -> 'b job) -> 'b job
  end

  (** Sequential run of a job *)
  val run: 'a job -> 'a

  (** Same as [run] but doesn't actually run any shell command,
      and feed a dummy result to the cont. *)
  val dry_run: 'a job -> 'a

  (** Converts a list of commands into a job that returns None on success, or
      the first failed command and its result.
      Unless [keep_going] is true, stops on first error. *)
  val of_list: ?keep_going:bool -> command list ->
    (command * OpamProcess.result) option job
end
