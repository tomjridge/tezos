(*****************************************************************************)
(*                                                                           *)
(* Open Source License                                                       *)
(* Copyright (c) 2021-2022 Tarides <contact@tarides.com>                     *)
(*                                                                           *)
(* Permission is hereby granted, free of charge, to any person obtaining a   *)
(* copy of this software and associated documentation files (the "Software"),*)
(* to deal in the Software without restriction, including without limitation *)
(* the rights to use, copy, modify, merge, publish, distribute, sublicense,  *)
(* and/or sell copies of the Software, and to permit persons to whom the     *)
(* Software is furnished to do so, subject to the following conditions:      *)
(*                                                                           *)
(* The above copyright notice and this permission notice shall be included   *)
(* in all copies or substantial portions of the Software.                    *)
(*                                                                           *)
(* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR*)
(* IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,  *)
(* FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL   *)
(* THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER*)
(* LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING   *)
(* FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER       *)
(* DEALINGS IN THE SOFTWARE.                                                 *)
(*                                                                           *)
(*****************************************************************************)

module Seq : sig
  include module type of Stdlib.Seq

  module Custom : sig
    val take : int -> 'a Seq.t -> 'a list

    val mapi64 : 'a Seq.t -> (int64 * 'a) Seq.t

    val take_up_to : is_last:('a -> bool) -> 'a Seq.t -> 'a Seq.t * 'a list

    val take_until : is_last:('a -> bool) -> 'a Seq.t -> 'a Seq.t
  end
end

(* val operations_of_block_level : int -> Tezos_metrics.block_info Lwt.t *)
(** Retrieve some basic informations on a given {i tezos mainnet} block level by
    querying the tezstats.com JSON API.

    Uses both in-memory and on-disk cache that are never emptied *)

(** See [Trace_stats_summary] for an explanation and an example.

    Heavily inspired by the "repr" library.

    Type parameters:

    - ['res] is the output of [finalise].
    - ['f] is the full contructor that creates a ['res].
    - ['v] is the output of [folder.finalise], one parameter of ['f].
    - ['rest] is ['f] or ['res] or somewhere in between.
    - ['acc] is the accumulator of one folder.
    - ['row] is what needs to be fed to all [folder.accumulate].

    Typical use case:

    {[
      let pf =
        open_ (fun res_a res_b -> my_constructor res_a res_b)
        |+ folder my_acc_a my_accumulate_a my_finalise_a
        |+ folder my_acc_b my_accumulate_b my_finalise_b
        |> seal
      in
      let res = my_row_sequence |> Seq.fold_left accumulate pf |> finalise in
    ]} *)
module Parallel_folders : sig
  (** Section 1/3 - Individual folders *)

  type ('row, 'acc, 'v) folder

  (** Create one folder to be passed to an open parallel folder using [|+]. *)
  val folder :
    'acc -> ('acc -> 'row -> 'acc) -> ('acc -> 'v) -> ('row, 'acc, 'v) folder

  (** Section 2/3 - Open parallel folder *)

  type ('res, 'row, 'v) folders

  type ('res, 'row, 'f, 'rest) open_t

  (** Start building a parallel folder. *)
  val open_ : 'f -> ('res, 'row, 'f, 'f) open_t

  (** Add a folder to an open parallel folder. *)
  val app :
    ('res, 'row, 'f, 'v -> 'rest) open_t ->
    ('row, 'acc, 'v) folder ->
    ('res, 'row, 'f, 'rest) open_t

  (** Alias for [app]. *)
  val ( |+ ) :
    ('res, 'row, 'f, 'v -> 'rest) open_t ->
    ('row, 'acc, 'v) folder ->
    ('res, 'row, 'f, 'rest) open_t

  (** Section 3/3 - Closed parallel folder *)

  type ('res, 'row) t

  (** Stop building a parallel folder.

      Gotcha: It may seal a partially applied [f]. *)
  val seal : ('res, 'row, 'f, 'res) open_t -> ('res, 'row) t

  (** Forward a row to all registered functional folders. *)
  val accumulate : ('res, 'row) t -> 'row -> ('res, 'row) t

  (** Finalise all folders and pass their result to the user-defined function
      provided to [open_]. *)
  val finalise : ('res, 'row) t -> 'res
end
