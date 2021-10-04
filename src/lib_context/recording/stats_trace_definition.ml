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

(** File format of a trace listing lib_context function call timings and
    various other statistics.

    Such files can be summarised to JSON in order to enable data analysis. *)

module V0 = struct
  type float32 = int32 [@@deriving repr]

  let version = 0

  (** Stats extracted from [Irmin_pack.Stats.get ()]. *)
  type pack = {
    finds_total : int;
    finds_from_staging : int;
    finds_from_lru : int;
    finds_from_pack_direct : int;
    finds_from_pack_indexed : int;
    cache_misses : int;
    appended_hashes : int;
    appended_offsets : int;
    inode_add : int;
    inode_remove : int;
    inode_of_seq : int;
    inode_of_raw : int;
    inode_rec_add : int;
    inode_rec_remove : int;
    inode_to_binv : int;
    inode_decode_bin : int;
    inode_encode_bin : int;
  }
  [@@deriving repr]

  (** Stats extracted from [Irmin.Tree.counters ()]. *)
  type tree = {
    contents_hash : int;
    contents_find : int;
    contents_add : int;
    contents_mem : int;
    node_hash : int;
    node_mem : int;
    node_index : int;
    node_add : int;
    node_find : int;
    node_val_v : int;
    node_val_find : int;
    node_val_list : int;
  }
  [@@deriving repr]

  (** Stats extracted from [Index.Stats.get ()].

    [new_merge_durations] is not just a mirror of
    [Index.Stats.merge_durations], it only contains the new entries since
    the last time it was recorded. This list is always empty when in the
    header. *)
  type index = {
    bytes_read : int;
    nb_reads : int;
    bytes_written : int;
    nb_writes : int;
    nb_merge : int;
    new_merge_durations : float list;
  }
  [@@deriving repr]

  (** Stats extracted from [Gc.quick_stat ()]. *)
  type gc = {
    minor_words : float;
    promoted_words : float;
    major_words : float;
    minor_collections : int;
    major_collections : int;
    heap_words : int;
    compactions : int;
    top_heap_words : int;
    stack_size : int;
  }
  [@@deriving repr]

  (** Stats extracted from filesystem. Requires the path to the irmin store. *)
  type disk = {
    index_data : int64;
    index_log : int64;
    index_log_async : int64;
    store_dict : int64;
    store_pack : int64;
  }
  [@@deriving repr]

  (** Stats extracted from getrusage *)
  type rusage = {
    utime : float;
    stime : float;
    maxrss : int64;
    minflt : int64;
    majflt : int64;
    inblock : int64;
    oublock : int64;
    nvcsw : int64;
    nivcsw : int64;
  }
  [@@deriving repr]

  (** Melting pot of stats, recorded before and after every commits.

    They are necessary in order to compute any throughput analytics. *)
  type bag_of_stats = {
    pack : pack;
    tree : tree;
    index : index;
    gc : gc;
    disk : disk;
    rusage : rusage;
    timestamp_wall : float;
    timestamp_cpu : float;
  }
  [@@deriving repr]

  (** Stats computed from the [tree] value passed to the commit operation,
    before the commit, when the tree still carries the modifications brought
    by the previous operations. *)
  type store_before = {
    nodes : int;
    leafs : int;
    skips : int;
    depth : int;
    width : int;
  }
  [@@deriving repr]

  type watched_node =
    [ `Contracts_index
    | `Big_maps_index
    | `Rolls_index
    | `Rolls_owner_current
    | `Commitments ]
  [@@deriving repr, enum]

  (** Stats computed on the [tree] value passed to the commit operation, after
    the commit, when the inode has been reconstructed and that [Tree.length]
    is now innexpensive to perform. *)
  type store_after = {watched_nodes_length : float list} [@@deriving repr]

  module Frequent_op = struct
    type tree =
      [ `Empty
      | `Of_raw
      | `Of_value
      | `Mem
      | `Mem_tree
      | `Find
      | `Is_empty
      | `Kind
      | `Hash
      | `Equal
      | `To_value
      | `Clear
      | `Find_tree
      | `List
      | `Add
      | `Add_tree
      | `Remove
      | `Fold ]
    [@@deriving repr]

    type tag =
      [ `Tree of tree
      | `Find_tree
      | `List
      | `Fold
      | `Add_tree
      | `Mem
      | `Mem_tree
      | `Find
      | `Get_protocol
      | `Hash
      | `Merkle_tree
      | `Find_predecessor_block_metadata_hash
      | `Find_predecessor_ops_metadata_hash
      | `Get_test_chain
      | `Exists
      | `Retrieve_commit_info
      | `Add
      | `Remove
      | `Add_protocol
      | `Add_predecessor_block_metadata_hash
      | `Add_predecessor_ops_metadata_hash
      | `Add_test_chain
      | `Remove_test_chain
      | `Fork_test_chain
      | `Checkout
      | `Checkout_exn
      | `Sync
      | `Set_master
      | `Set_head
      | `Clear_test_chain
      | `Commit_test_chain_genesis
      | `Init
      | `Restore_context ]
    [@@deriving repr]

    type payload = float32 [@@deriving repr]
  end

  module Stats_op = struct
    type payload = {
      duration : float32;
      before : bag_of_stats;
      after : bag_of_stats;
    }
    [@@deriving repr]
  end

  module Commit_op = struct
    (** Specs of a commit, this is only available when replaying, not when
      playing *)
    type specs = {
      level : int;
      tzop_count : int;
      tzop_count_tx : int;
      tzop_count_contract : int;
      tz_gas_used : int;
      tz_storage_size : int;
      tz_cycle_snapshot : int;
      tz_time : int;
      tz_solvetime : int;
      ev_count : int;
      uses_patch_context : bool;
    }
    [@@deriving repr]

    type payload = {
      duration : float32;
      before : bag_of_stats;
      after : bag_of_stats;
      store_before : store_before;
      store_after : store_after;
      specs : specs option;
    }
    [@@deriving repr]
  end

  (** Stats gathered while running an operation.

    The number of rows in a stats trace and the presence of [`Close] is not
    known in advance. If [`Close] is present in the trace, it usually is the
    last operation and it follows a [`Commit].

    For simplicity, the commit genesis and commit operations are represented
    as one.

    {3 Operation Durations}

    For each operation we record its wall time length using a [float32], a
    [float16] would be suitable too (it would have >3 digits of precision). *)
  type row =
    [ `Frequent_op of Frequent_op.tag * Frequent_op.payload
    | `Commit of Commit_op.payload
    | `Close of Stats_op.payload
    | `Dump_context of Stats_op.payload ]
  [@@deriving repr]

  (** Stats extracted from [Gc.get ()]. *)
  type gc_control = {
    minor_heap_size : int;
    major_heap_increment : int;
    space_overhead : int;
    verbose : int;
    max_overhead : int;
    stack_limit : int;
    allocation_policy : int;
    window_size : int;
    custom_major_ratio : int;
    custom_minor_ratio : int;
    custom_minor_max_size : int;
  }
  [@@deriving repr]

  (** Informations gathered from the tezos node.

    Noting so far. Any ideas? *)
  type setup_play = unit [@@deriving repr]

  (** Informations gathered from replay parameters.

    The [message] field should typically be a JSON string containing the
    [git describe] of the versions of irmin, index, etc... *)
  type setup_replay = {
    initial_block_level : int option;
    replayable_trace_version : int;
  }
  [@@deriving repr]

  type config = {
    store_type : [`Pack | `Pack_layered | `Pack_mem];
    setup : [`Play of setup_play | `Replay of setup_replay];
    message : string option;
  }
  [@@deriving repr]

  (** File header.

    {3 Timestamps}

    [initial_stats.timestamp_wall] and [initial_stats.timestamp_cpu] are the
    starting points of the trace, they are to be substracted from their
    counterpart in [commit] to compute time spans.

    [timeofday] is the date and time at which the stats started to be
    accumulated.

    [initial_stats.timestamp_wall] originate from [Mtime_clock.now].

    [initial_stats.timestamp_cpu] originate from [Sys.time]. *)
  type header = {
    config : config;
    hostname : string;
    timeofday : float;
    word_size : int;
    os_type : string;
    big_endian : bool;
    gc_control : gc_control;
    runtime_parameters : string;
    backend_type : [`Native | `Bytecode | `Other of string];
    ocaml_version : string;
    initial_stats : bag_of_stats;
  }
  [@@deriving repr]
end

module Latest = V0
include Latest

let watched_nodes : watched_node list =
  Stdlib.List.init (max_watched_node + 1) (fun i ->
      watched_node_of_enum i |> Stdlib.Option.get)

let step_list_per_watched_node =
  let aux = function
    | `Contracts_index -> ["data"; "contracts"; "index"]
    | `Big_maps_index -> ["data"; "big_maps"; "index"]
    | `Rolls_index -> ["data"; "rolls"; "index"]
    | `Rolls_owner_current -> ["data"; "rolls"; "owner"; "current"]
    | `Commitments -> ["data"; "commitments"]
  in
  Stdlib.List.combine watched_nodes (List.map aux watched_nodes)

let path_per_watched_node =
  List.map
    (fun (k, l) -> (k, "/" ^ Stdlib.String.concat "/" l))
    step_list_per_watched_node

include Trace_auto_file_format.Make (struct
  module Latest = Latest

  (** Irmin's Stats Bootstrap Trace *)
  let magic = Trace_auto_file_format.Magic.of_string "TezosSta"

  let get_version_converter = function
    | 0 ->
        Trace_auto_file_format.create_version_converter
          ~header_t:V0.header_t
          ~row_t:V0.row_t
          ~upgrade_header:Fun.id
          ~upgrade_row:Fun.id
    | i -> Fmt.invalid_arg "Unknown Stats_trace version %d" i
end)
