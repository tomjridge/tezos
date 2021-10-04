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

(** Traces file format definitions.

    In addition to Irmin, this file is meant to be used from Octez. A specific
    OCaml version and the 32bit architecture should be supported.

    {3 Versioning}

    All file formats defined here contain a version number in order to allow for
    backward compatibility or gracefuly fail when dealing with outdated files.

    This mechanism addresses the situation where a file format needs to evolve
    while tezos-node users are still able to generate older versions of that
    file format.

    Changing something within this file may break the "decoding shape" of a
    version of a file. the parsing of existing files. Only proceede if you know
    what you are doing!!!

    The way the trace is constructed for a version should be stable too.

    See trace_common.ml for a lower level view of the file formats.

    {3 Traces Workflow}

    {v
            (tezos-node) -------------------------------------->\
                  |                                             |
                  v                                             |
          [raw actions trace] -> (manage_actions.exe summarise) |
                  |                            |                |
                  |                            v                |
                  |                 [raw_actions_summary.json]  |
                  |                            |                |
                  |                            v                |
                  |                   (pandas / matplotlib)     |
                  v                                             |
  (manage_actions.exe to-replayable)                            |
                  |                                             |
                  v                                             |
       [replayable actions trace]                               |
                  |                                             |
                  v                                             v
             (tree.exe) -----------------------------> [stats trace]
                                                            |
                                                            v
                                               (manage_stats.exe summarise)
                                                            |
                                                            v
                                                  [stats_summary.json]
                                                            |
                                                            v
                                     (pandas / matplotlib / manage_stats.exe pp)
    v}

    {3 Events and Operations}

    Each [row] in [Raw_actions_trace] represents an event at the level of the
    lib_context API. Such an event may be a simple operation, like [Checkout],
    or a control flow event like [Fold_step_enter] when lib_context calls the
    callback during a [fold], or [Fold_step_exit] when the user returns a result
    to the same callback.

    Each [row] in [Replayable_actions_trace] has the same meaning as in
    [Raw_actions_trace] (i.e. it is an event), except that these events have
    been adapted to be easily replayable.

    Each [row] in [Stats_trace] represents an operation, there are no more
    control flow events.

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

(** Use Option from Stdlib instead of the Tezos one. *)
module Option = Stdlib.Option

(** Use List from Stdlib instead of the Tezos one. *)
module List = Stdlib.List

(** Use failwith from Stdlib instead of the Tezos one. *)
let failwith = Stdlib.failwith

(** Import from recording to avoid duplication. *)
module Trace_common = Tezos_context_recording.Trace_common

module Trace_auto_file_format = Tezos_context_recording.Trace_auto_file_format

open struct
  module Seq = Trace_common.Seq
end

(** [Replayable_actions_trace], a ready to replay trace of Tezos's interactions
    with lib_context.

    {3 Interleaved Contexts and Commits}

    All the recorded operations in Tezos operate on (and create new) immutable
    records of type [context]. Most of the time, everything is linear (i.e. the
    input context to an operation is the latest output context), but there
    sometimes are several parallel chains of contexts, where all but one will
    end up being discarded.

    The same goes for values of type [tree].

    Similarly to contexts, commits are not always linear, i.e. a checkout may
    choose a parent that is not the latest commit, making the previous block an
    orphan one.

    To solve this conundrum when replaying the trace, we need to remember all
    the [context_id -> context_value] and
    [trace commit hash -> real commit hash] pairs to make sure an operation is
    operating on the right parent.

    In the trace, the context indices and the commit hashes are 'scoped',
    meaning that they are tagged with informations indicating if this is the
    very first or very last occurence of that value in the trace.

    In practice, there is between 0 and 1 context/tree/commit in cache, and
    rarely 2+. *)

module V0 = struct
  let version = 0

  type key = string list [@@deriving repr]

  type hash = string [@@deriving repr]

  type message = string [@@deriving repr]

  type varint63 = Optint.Int63.t [@@deriving repr]

  let varint63_t =
    let module V = Repr.Binary.Varint_int63 in
    Repr.like ~bin:(V.encode, V.decode, Obj.magic V.sizer) varint63_t
  (* FIXME: wait for Repr modification to support size in like *)

  type tracker = varint63 [@@deriving repr]

  type step = string [@@deriving repr]

  type value = bytes [@@deriving repr]

  type depth = [`Eq of int | `Ge of int | `Gt of int | `Le of int | `Lt of int]
  [@@deriving repr]

  type order = [`Sorted | `Undefined] [@@deriving repr]

  type block_level = int [@@deriving repr]

  type time_protocol = int64 [@@deriving repr]

  type merkle_leaf_kind = [`Hole | `Raw_context] [@@deriving repr]

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

  (** [scope_start_rhs] tags are used in replay to identify the situations
        where a hash is created but has already been created, i.e. when two
        commits have the same hash. *)
  type scope_start_rhs = First_instanciation | Reinstanciation
  [@@deriving repr]

  (** [scope_start_lhs] tags are used in replay to identify the situations
        where a hash is required but was never seen, e.g. the first checkout of
        a replay that starts on a snapshot. *)
  type scope_start_lhs = Instanciated | Not_instanciated [@@deriving repr]

  (** [scope_end] tags are used in replay to garbage collect the values in
        cache. *)
  type scope_end = Last_occurence | Will_reoccur [@@deriving repr]

  type tree = scope_end * tracker [@@deriving repr]

  type context = scope_end * tracker [@@deriving repr]

  type commit_hash_rhs = scope_start_rhs * scope_end * hash [@@deriving repr]

  type commit_hash_lhs = scope_start_lhs * scope_end * hash [@@deriving repr]

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
      | Find of (tree * key, bool (* recording is_some *)) fn
      | Is_empty of (tree, bool) fn
      | Kind of (tree, [`Tree | `Value]) fn
      | Hash of (tree, unit (* not recorded *)) fn
      | Equal of (tree * tree, bool) fn
      | To_value of (tree, bool (* recording is_some *)) fn
      | Clear of (int option * tree, unit) fn
      (* [io __ ___] *)
      | Find_tree of (tree * key, tree option) fn
      | Add of (tree * key * value, tree) fn
      | Add_tree of (tree * key * tree, tree) fn
      | Remove of (tree * key, tree) fn
    [@@deriving repr]
  end

  type event =
    (* [** __ ___] *)
    | Tree of Tree.t
    (* [_o i_ ___] *)
    | Find_tree of (context * key, tree option) fn
    | Fold_start of (depth option * order) * context * key
    | Fold_step_enter of tree (* not recording step *)
    | Fold_step_exit of tree
    | Fold_end
    (* [i_ io ___]*)
    | Add_tree of (context * key * tree, context) fn
    (* [__ i_ ___] *)
    | Mem of (context * key, bool) fn
    | Mem_tree of (context * key, bool) fn
    | Find of (context * key, bool (* recording is_some *)) fn
    | Get_protocol of (context, unit (* not recorded *)) fn
    | Hash of
        (time_protocol * message option * context, unit (* not recorded *)) fn
    | Find_predecessor_block_metadata_hash of
        (context, unit (* not recorded, could have recorded is_some *)) fn
    | Find_predecessor_ops_metadata_hash of
        (context, unit (* not recorded, could have recorded is_some *)) fn
    | Get_test_chain of (context, unit (* not recorded *)) fn
    (* [__ __ i__] *)
    | Exists of (commit_hash_lhs, bool) fn
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
    | Checkout of (commit_hash_lhs, context) fn
    (* [__ __ i_m] *)
    | Commit_genesis_start of (chain_id * time_protocol * hash, unit) fn
    | Commit_genesis_end of (unit, commit_hash_rhs) fn
    | Clear_test_chain of (chain_id, unit) fn
    (* [__ i_ __m] *)
    | Commit of (time_protocol * message option * context, commit_hash_rhs) fn
    (* [__ ~~ _o_] *)
    | Init of (bool, unit) fn
    | Patch_context_enter of context
    | Patch_context_exit of context * context
  [@@deriving repr]

  (** Events of a block. The first/last are either init/commit_genesis or
        checkout(exn)/commit. *)
  type row = {
    level : int;
    tzop_count : int;
    tzop_count_tx : int;
    tzop_count_contract : int;
    tz_gas_used : int;
    tz_storage_size : int;
    tz_cycle_snapshot : int;
    tz_time : int;
    tz_solvetime : int;
    ops : event array;
    uses_patch_context : bool;
  }
  [@@deriving repr]

  type header = {
    initial_block : (block_level * hash) option;
    last_block : block_level * hash;
    block_count : int;
  }
  [@@deriving repr]
end

module Latest = V0
include Latest

include Trace_auto_file_format.Make (struct
  module Latest = Latest

  (** Irmin's Replayable Bootstrap Trace *)
  let magic = Trace_auto_file_format.Magic.of_string "TezosRep"

  let get_version_converter = function
    | 0 ->
        Trace_auto_file_format.create_version_converter
          ~header_t:V0.header_t
          ~row_t:V0.row_t
          ~upgrade_header:Fun.id
          ~upgrade_row:Fun.id
    | i -> Fmt.invalid_arg "Unknown replayable actions trace version %d" i
end)
