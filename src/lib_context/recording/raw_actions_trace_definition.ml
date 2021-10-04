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

(** File format of a trace listing the interactions between lib_context and
    the rest of Octez.

    {3 Use Cases}

    - Conversion to [Replayable_actions_trace] for benchmarking.
    - Direct analysis for debuging.
    - Get insighs on the access patterns of Octez with lib_context.

    {3 Events sorting}

    The events are grouped and sorted given the content of their input and
    output. A 7 char code represents a category. The code matches the following
    pattern:

    {v [i_][o_] [i_][o_] [i_][o_][m_] v}

    The meaning of each char is the following:

    - 1. Does the operation takes a [Context.tree]
    - 2. Does the operation outputs a [Context.tree]
    - 3. Does the operation takes a [Context.t]
    - 4. Does the operation outputs a [Context.t]
    - 5. Does the operation takes a [Context.index]
    - 6. Does the operation outputs a [Context.index]
    - 7. Does the operation mutates a [Context.index]

    Thanks to this grouping, it appears clearly that [Add_tree] is the only
    operation that combines trees into contexts (its domain is [i_ io ___]). *)

(** Latest raw actions trace version. Modifications to this module shouldn't
    break compatibilty with existing files using this format. *)
module V0 = struct
  let version = 0

  type header = unit [@@deriving repr]

  type key = string list [@@deriving repr]

  type hash = string [@@deriving repr]

  type commit_hash = hash [@@deriving repr]

  type message = string [@@deriving repr]

  type varint63 = Optint.Int63.t [@@deriving repr]

  let varint63_t =
    let module V = Repr.Binary.Varint_int63 in
    Repr.like ~bin:(V.encode, V.decode, Obj.magic V.sizer) varint63_t
  (* FIXME: wait for Repr modification to support size in like *)

  type tracker = varint63 [@@deriving repr]

  type tracker_range = {first_tracker : tracker; count : int} [@@deriving repr]

  type tree = tracker [@@deriving repr]

  type trees_with_contiguous_trackers = tracker_range [@@deriving repr]

  type context = tracker [@@deriving repr]

  type value = bytes [@@deriving repr]

  type md5 = Digestif.MD5.t

  let md5_t : md5 Repr.t =
    Repr.map Repr.string Digestif.MD5.of_raw_string Digestif.MD5.to_raw_string

  (** Type of [value] output of lib_context. Can be reused during replay or
      trace inspection to assert correcness or returned value.

      The [`Hash] tag is used when the byte sequence is long. *)
  type output_value = [`Value of bytes | `Hash of md5] [@@deriving repr]

  type step = string [@@deriving repr]

  type depth = [`Eq of int | `Ge of int | `Gt of int | `Le of int | `Lt of int]
  [@@deriving repr]

  type order = [`Sorted | `Undefined] [@@deriving repr]

  type block_level = int32 [@@deriving repr]

  type time_protocol = int64 [@@deriving repr]

  type merkle_leaf_kind = Block_services.merkle_leaf_kind

  let merkle_leaf_kind_t =
    Repr.enum
      "merkle_leaf_kind"
      [
        ("Hole", Block_services.Hole);
        ("Raw_context", Block_services.Raw_context);
      ]

  type chain_id = string [@@deriving repr]

  type test_chain_status = Test_chain_status.t

  let test_chain_status_t =
    let open Repr in
    variant "test_chain_status" (fun not_running forking running -> function
      | Test_chain_status.Not_running -> not_running
      | Forking {protocol; expiration} ->
          let protocol = Protocol_hash.to_string protocol in
          let expiration = Time.Protocol.to_seconds expiration in
          forking (protocol, expiration)
      | Running {chain_id; genesis; protocol; expiration} ->
          let chain_id = Chain_id.to_string chain_id in
          let genesis = Block_hash.to_string genesis in
          let protocol = Protocol_hash.to_string protocol in
          let expiration = Time.Protocol.to_seconds expiration in
          running ((chain_id, genesis), (protocol, expiration)))
    |~ case0 "Not_running" Test_chain_status.Not_running
    |~ case1
         "Forking"
         (pair hash_t time_protocol_t)
         (fun (protocol, expiration) ->
           let protocol = Protocol_hash.of_string_exn protocol in
           let expiration = Time.Protocol.of_seconds expiration in
           Test_chain_status.Forking {protocol; expiration})
    |~ case1
         "Running"
         (pair (pair chain_id_t hash_t) (pair hash_t time_protocol_t))
         (fun ((chain_id, genesis), (protocol, expiration)) ->
           let chain_id = Chain_id.of_string_exn chain_id in
           let genesis = Block_hash.of_string_exn genesis in
           let protocol = Protocol_hash.of_string_exn protocol in
           let expiration = Time.Protocol.of_seconds expiration in
           Test_chain_status.Running {chain_id; genesis; protocol; expiration})
    |> sealv

  type system_wide_timestamp = float [@@deriving repr]

  (** System timestamps recorded in order to correlate the parallel calls
      of the various processes accessing the context. *)
  type timestamp_bounds = {
    before : system_wide_timestamp;
    after : system_wide_timestamp;
  }
  [@@deriving repr]

  type ('input, 'output) fn = 'input * 'output [@@deriving repr]

  module Tree = struct
    type raw = [`Value of value | `Tree of (step * raw) list] [@@deriving repr]

    type t =
      (* [_o i_ ___] *)
      | Empty of (context, tree) fn
      | Of_value of (context * value, tree) fn
      (* [_o __ ___] *)
      | Of_raw of (raw, tree) fn
      (* [i_ __ ___] *)
      | Mem of (tree * key, bool) fn
      | Mem_tree of (tree * key, bool) fn
      | Find of (tree * key, output_value option (* partially recording *)) fn
      | Is_empty of (tree, bool) fn
      | Kind of (tree, [`Tree | `Value]) fn
      | Hash of (tree, hash) fn
      | Equal of (tree * tree, bool) fn
      | To_value of (tree, output_value option (* partially recording *)) fn
      | Clear of (int option * tree, unit) fn
      (* [io __ ___] *)
      | Find_tree of (tree * key, tree option) fn
      | List of
          ( tree * int option * int option,
            trees_with_contiguous_trackers (* not recording the steps *) )
          fn
      | Add of (tree * key * value, tree) fn
      | Add_tree of (tree * key * tree, tree) fn
      | Remove of (tree * key, tree) fn
      | Fold_start of (depth option * order) * tree * key
      | Fold_step_enter of tree (* not recording step *)
      | Fold_step_exit of tree
      | Fold_end of int
    [@@deriving repr]
  end

  type row =
    (* [** __ ___] *)
    | Tree of Tree.t
    (* [_o i_ ___] *)
    | Find_tree of (context * key, tree option) fn
    | List of
        ( context * int option * int option,
          trees_with_contiguous_trackers (* not recording the steps *) )
        fn
    | Fold_start of (depth option * order) * context * key
    | Fold_step_enter of tree (* not recording step *)
    | Fold_step_exit of tree
    | Fold_end of int
    (* [i_ io ___] *)
    | Add_tree of (context * key * tree, context) fn
    (* [__ i_ ___] *)
    | Mem of (context * key, bool) fn
    | Mem_tree of (context * key, bool) fn
    | Find of (context * key, output_value option (* partially recording *)) fn
    | Get_protocol of (context, hash) fn
    | Hash of (time_protocol * message option * context, commit_hash) fn
    | Merkle_tree of
        ( context * merkle_leaf_kind * step list,
          unit (* not recording the block_service.merkle_tree *) )
        fn
    | Find_predecessor_block_metadata_hash of (context, hash option) fn
    | Find_predecessor_ops_metadata_hash of (context, hash option) fn
    | Get_test_chain of (context, test_chain_status) fn
    (* [__ __ i__] *)
    | Exists of (commit_hash, bool) fn * timestamp_bounds
    | Retrieve_commit_info of
        (commit_hash, bool (* only recording is_ok for that massive tuple *)) fn
        * timestamp_bounds
    (* [__ io ___] *)
    | Add of (context * key * value, context) fn
    | Remove of (context * key, context) fn
    | Add_protocol of (context * hash, context) fn
    | Add_predecessor_block_metadata_hash of (context * hash, context) fn
    | Add_predecessor_ops_metadata_hash of (context * hash, context) fn
    | Add_test_chain of (context * test_chain_status, context) fn
    | Remove_test_chain of (context, context) fn
    | Fork_test_chain of (context * hash * time_protocol, context) fn
    (* [__ _o i__] *)
    | Checkout of (commit_hash, context option) fn * timestamp_bounds
    | Checkout_exn of
        (commit_hash, (context, unit) result) fn * timestamp_bounds
    (* [__ __ i_m] *)
    | Close
    | Sync of timestamp_bounds
    | Set_master of (commit_hash, unit) fn
    | Set_head of (chain_id * commit_hash, unit) fn
    | Commit_genesis_start of
        (chain_id * time_protocol * hash, unit) fn * system_wide_timestamp
    | Commit_genesis_end of
        (unit, (commit_hash, unit) result) fn * system_wide_timestamp
    | Clear_test_chain of (chain_id, unit) fn
    (* [__ i_ __m] *)
    | Commit of
        (time_protocol * message option * context, commit_hash) fn
        * timestamp_bounds
    | Commit_test_chain_genesis of
        ( context * (time_protocol * block_level * hash),
          unit (* block header not recorded *) )
        fn
        * timestamp_bounds
    (* [__ ~~ _o_] *)
    | Init of (bool, unit) fn
    | Patch_context_enter of context
    | Patch_context_exit of context * (context, unit) result
    (* [__ __ i__] *)
    | Dump_context of timestamp_bounds
    (* special *)
    | Unhandled of Recorder.unhandled
  [@@deriving repr]
end

module Latest = V0
include Latest

include Trace_auto_file_format.Make (struct
  module Latest = Latest

  (** Irmin's Raw Bootstrap Trace

      The meaning is a bit stale but let's preserve backward compatibility with
      existing files.
   *)
  let magic = Trace_auto_file_format.Magic.of_string "TezosRaw"

  let get_version_converter = function
    | 0 ->
        Trace_auto_file_format.create_version_converter
          ~header_t:V0.header_t
          ~row_t:V0.row_t
          ~upgrade_header:Fun.id
          ~upgrade_row:Fun.id
    | i ->
        let msg = Fmt.str "Unknown Raw_actions_trace version %d" i in
        raise (Misc.Suspicious_trace_file msg)
end)

type file_type = [`Ro | `Rw | `Misc]

let take : type a. int -> a Seq.t -> a list =
 fun count_expected seq ->
  let rec aux seq took_rev count_sofar =
    if count_sofar = count_expected then List.rev took_rev
    else
      match seq () with
      | Seq.Nil ->
          (* Early stop because seq too small *)
          List.rev took_rev
      | Cons (v, seq) -> aux seq (v :: took_rev) (count_sofar + 1)
  in
  aux seq [] 0

(** Automatic classification of a raw trace file depending on the number and
      kind of [Init] operations in the first 10 rows of the trace. *)
let type_of_file p =
  let (_, _, reader) = open_reader p in
  let l = take 10 reader in
  let ros = List.filter (function Init (true, _) -> true | _ -> false) l in
  let rws = List.filter (function Init (false, _) -> true | _ -> false) l in
  match (List.length ros, List.length rws) with
  | (1, 0) -> `Ro
  | (0, 1) -> `Rw
  | _ -> `Misc

let trace_files_of_trace_directory ?filter prefix : (string * int) list =
  let filter =
    match filter with
    | None -> [`Ro; `Rw; `Misc]
    | Some v -> (v :> file_type list)
  in
  let parse_filename p =
    match String.split_on_char '.' p with
    | ["raw_actions_trace"; pid; "trace"] -> int_of_string_opt pid
    | _ -> None
  in
  Sys.readdir prefix |> Array.to_list
  |> List.filter_map (fun p ->
         match parse_filename p with
         | Some pid -> Some (Filename.concat prefix p, pid)
         | None -> None)
  |> List.filter (fun (p, _) -> List.mem ~equal:( = ) (type_of_file p) filter)
