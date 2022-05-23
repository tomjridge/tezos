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

(** The signature for a recorder. It defines one observer for each [Context]
    event where an event is either a function call or a callback call.

    The [Shim] calls an observer in two stages. The input arguments will be
    applied before the lib_context call is performed and the output argument
    will be applied when lib_context returns. Most observers in
    [Raw_actions_trace] don't take advantage of that trick as they just push
    a row to the trace when the lib_context operation is over, however, all
    observers in [Stats_trace] use this trick to record time at both stages.

    Idea: Share this signature with [Context_sigs.S] by using the following
    trick: [module type OPERATIONS = S with type 'a output := 'a],
    [module type RECORDER = S with type 'a output := 'a -> unit]. *)

(** All the cases that are not handled by a recorder.

    WARNING: Do not ever remove tag from this list, only add new ones. *)
type unhandled =
  | Tree_config
  | Tree_kinded_key
  | Tree_shallow
  | Tree_to_raw
  | Tree_pp
  | Tree_length
  | Tree_stats
  | Length
  | Stats
  | Restore_context
  | Check_protocol_commit_consistency
  | Validate_context_hash_consistency_and_commit
  | Legacy_restore_context
  | Legacy_restore_contexts
  | Legacy_get_protocol_data_from_header
  | Legacy_dump_snapshot
  | Produce_tree_proof
  | Verify_tree_proof
  | Produce_stream_proof
  | Verify_stream_proof
  | Index_empty
  | Context_is_empty
  | Config
  | Equal_config
  | To_memory_tree
[@@deriving repr]

module type S = sig
  module Impl : Tezos_context_sigs.Context.MACHIN

  (** A [Context.tree] alongside a unique identifier. *)
  type tree = Impl.tree * Optint.Int63.t

  (** A [Context.t] alongside a unique identifier. *)
  type context = Impl.context * Optint.Int63.t

  (** Return type of most observers *)
  type 'res output := 'res -> unit

  (** Return type of some most observers that need the Lwt scheduler *)
  type 'res output_lwt := ('res -> unit Lwt.t) Lwt.t

  (** Observer function that notifies the call to an unhandled [Context]
      function. *)
  val unhandled : unhandled -> _ output

  module Tree : sig
    val empty : context -> tree output

    val of_raw : Impl.Tree.raw -> tree output

    val of_value : context -> Impl.value -> tree output

    val mem : tree -> Impl.key -> bool output

    val mem_tree : tree -> Impl.key -> bool output

    val find : tree -> Impl.key -> Impl.value option output

    val is_empty : tree -> bool output

    val kind : tree -> [`Tree | `Value] output

    val hash : tree -> Context_hash.t output

    val equal : tree -> tree -> bool output

    val to_value : tree -> Impl.value option output

    val clear : depth:int option -> tree -> unit output

    val find_tree : tree -> Impl.key -> tree option output

    val list :
      tree ->
      offset:int option ->
      length:int option ->
      (string * tree) list output

    val add : tree -> Impl.key -> Impl.value -> tree output

    val add_tree : tree -> Impl.key -> tree -> tree output

    val remove : tree -> Impl.key -> tree output

    val fold :
      depth:
        [`Eq of int | `Ge of int | `Gt of int | `Le of int | `Lt of int] option ->
      order:[`Sorted | `Undefined] ->
      tree ->
      Impl.key ->
      int output

    val fold_step : int -> tree -> unit output
  end

  val find_tree : context -> Impl.key -> tree option output

  val list :
    context ->
    offset:int option ->
    length:int option ->
    (string * tree) list output

  val fold :
    depth:
      [`Eq of int | `Ge of int | `Gt of int | `Le of int | `Lt of int] option ->
    order:[`Sorted | `Undefined] ->
    context ->
    Impl.key ->
    int output

  val fold_step : int -> tree -> unit output

  val add_tree : context -> Impl.key -> tree -> context output

  val mem : context -> Impl.key -> bool output

  val mem_tree : context -> Impl.key -> bool output

  val find : context -> Impl.key -> Impl.value option output

  val get_protocol : context -> Protocol_hash.t output

  val hash :
    time:Time.Protocol.t ->
    message:string option ->
    context ->
    Context_hash.t output

  val merkle_tree :
    context ->
    Block_services.merkle_leaf_kind ->
    string list ->
    Block_services.merkle_tree output

  val find_predecessor_block_metadata_hash :
    context -> Block_metadata_hash.t option output

  val find_predecessor_ops_metadata_hash :
    context -> Operation_metadata_list_list_hash.t option output

  val get_test_chain : context -> Test_chain_status.t output

  val exists : Impl.index -> Context_hash.t -> bool output

  val retrieve_commit_info :
    Impl.index ->
    Block_header.t ->
    (Protocol_hash.t
    * string
    * string
    * Time.Protocol.t
    * Test_chain_status.t
    * Context_hash.t
    * Block_metadata_hash.t option
    * Operation_metadata_list_list_hash.t option
    * Context_hash.t list)
    tzresult
    output

  val add : context -> Impl.key -> Impl.value -> context output

  val remove : context -> Impl.key -> context output

  val add_protocol : context -> Protocol_hash.t -> context output

  val add_predecessor_block_metadata_hash :
    context -> Block_metadata_hash.t -> context output

  val add_predecessor_ops_metadata_hash :
    context -> Operation_metadata_list_list_hash.t -> context output

  val add_test_chain : context -> Test_chain_status.t -> context output

  val remove_test_chain : context -> context output

  val fork_test_chain :
    context ->
    protocol:Protocol_hash.t ->
    expiration:Time.Protocol.t ->
    context output

  val checkout : Impl.index -> Context_hash.t -> context option output

  val checkout_exn :
    Impl.index -> Context_hash.t -> (context, exn) result output

  val close : Impl.index -> unit output

  val sync : Impl.index -> unit output

  val set_master : Impl.index -> Context_hash.t -> unit output

  val set_head : Impl.index -> Chain_id.t -> Context_hash.t -> unit output

  val commit_genesis :
    Impl.index ->
    chain_id:Chain_id.t ->
    time:Time.Protocol.t ->
    protocol:Protocol_hash.t ->
    Context_hash.t tzresult output_lwt

  val clear_test_chain : Impl.index -> Chain_id.t -> unit output

  val commit :
    time:Time.Protocol.t ->
    message:string option ->
    context ->
    Context_hash.t output_lwt

  val commit_test_chain_genesis :
    context -> Block_header.t -> Block_header.t output

  val init :
    readonly:bool option ->
    ?indexing_strategy:[`Minimal | `Always] ->
    string ->
    Impl.index output

  val patch_context : context -> context tzresult output

  val restore_context :
    Impl.index ->
    expected_context_hash:Context_hash.t ->
    nb_context_elements:int ->
    fd:Lwt_unix.file_descr ->
    legacy:bool ->
    in_memory:bool ->
    progress_display_mode:Animation.progress_display_mode ->
    unit tzresult output_lwt

  val dump_context :
    Impl.index ->
    Context_hash.t ->
    fd:Lwt_unix.file_descr ->
    on_disk:bool ->
    progress_display_mode:Animation.progress_display_mode ->
    int tzresult output_lwt
end
