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

let ( let* ) = ( >>= )

let ( let+ ) = ( >|= )

module Def = Stats_trace_definition

(** Stats trace writer, to be instanciated from replay or from tezos-node using
    [Make] (below). *)
module Writer (Impl : Tezos_context_sigs.Context.S) = struct
  let is_darwin =
    try
      match Unix.open_process_in "uname" |> input_line with
      | "Darwin" -> true
      | _ -> false
    with Unix.Unix_error _ -> false

  (** Imperative stats trace collector. It is optimised to minimise the number
      allocated ocaml blocks. *)
  type t = {
    writer : Def.writer;
    store_path : string;
    mutable direct_timer : Mtime_clock.counter;
    mutable recursive_timers : Mtime.Span.t list;
    mutable prev_merge_durations : float list;
    mutable bag_before : Def.bag_of_stats;
    mutable store_before : Def.store_before;
  }

  let gc_control () =
    let open Gc in
    let v = get () in
    Def.
      {
        minor_heap_size = v.minor_heap_size;
        major_heap_increment = v.major_heap_increment;
        space_overhead = v.space_overhead;
        verbose = v.verbose;
        max_overhead = v.max_overhead;
        stack_limit = v.stack_limit;
        allocation_policy = v.allocation_policy;
        window_size = v.window_size;
        custom_major_ratio = v.custom_major_ratio;
        custom_minor_ratio = v.custom_minor_ratio;
        custom_minor_max_size = v.custom_minor_max_size;
      }

  module Bag_of_stats = struct
    let pack () =
      let open Irmin_pack.Stats in
      let v = get () in
      let cache_misses = Find.cache_misses v.finds in
      Def.
        {
          finds_total = v.finds.total;
          finds_from_staging = v.finds.from_staging;
          finds_from_lru = v.finds.from_lru;
          finds_from_pack_direct = v.finds.from_pack_direct;
          finds_from_pack_indexed = v.finds.from_pack_indexed;
          cache_misses;
          appended_hashes = v.appended_hashes;
          appended_offsets = v.appended_offsets;
          inode_add = v.inode_add;
          inode_remove = v.inode_remove;
          inode_of_seq = v.inode_of_seq;
          inode_of_raw = v.inode_of_raw;
          inode_rec_add = v.inode_rec_add;
          inode_rec_remove = v.inode_rec_remove;
          inode_to_binv = v.inode_to_binv;
          inode_decode_bin = v.inode_decode_bin;
          inode_encode_bin = v.inode_encode_bin;
        }

    let tree () =
      (* This call directly targets [Impl], it will not be recorded *)
      let v = Impl.module_tree_stats () in
      Def.
        {
          contents_hash = v.contents_hash;
          contents_find = v.contents_find;
          contents_add = v.contents_add;
          contents_mem = v.contents_mem;
          node_hash = v.node_hash;
          node_mem = v.node_mem;
          node_index = v.node_index;
          node_add = v.node_add;
          node_find = v.node_find;
          node_val_v = v.node_val_v;
          node_val_find = v.node_val_find;
          node_val_list = v.node_val_list;
        }

    let index prev_merge_durations =
      let open Index.Stats in
      let v = get () in
      let new_merge_durations =
        if v.merge_durations == prev_merge_durations then []
        else
          (* This is anoying to compute. We can't rely on nb_merge.
             Let's assume that all merge durations are unique.
             Let's assume that we never have >10 merges at once.

             And improvement of [Index.Stats] are on their way.
          *)
          let rec aux acc = function
            | [] -> acc
            | hd :: tl ->
                if List.mem ~equal:Float.equal hd prev_merge_durations then (
                  assert (acc = []) (* No oldie after a newies *) ;
                  aux acc tl)
                else aux ((hd /. 1e6) :: acc) tl
          in
          let l = aux [] v.merge_durations in
          assert (l <> []) (* At least one newie *) ;
          l
      in
      Def.
        {
          bytes_read = v.bytes_read;
          nb_reads = v.nb_reads;
          bytes_written = v.bytes_written;
          nb_writes = v.nb_writes;
          nb_merge = v.nb_merge;
          new_merge_durations;
        }

    let gc () =
      let open Gc in
      let v = quick_stat () in
      Def.
        {
          minor_words = v.minor_words;
          promoted_words = v.promoted_words;
          major_words = v.major_words;
          minor_collections = v.minor_collections;
          major_collections = v.major_collections;
          heap_words = v.heap_words;
          compactions = v.compactions;
          top_heap_words = v.top_heap_words;
          stack_size = v.stack_size;
        }

    let size_of_file path =
      let open Unix.LargeFile in
      try (stat path).st_size with Unix.Unix_error _ -> 0L

    let disk store_path =
      let ( / ) left right = Filename.concat left right in
      Def.
        {
          index_data = store_path / "index" / "data" |> size_of_file;
          index_log = store_path / "index" / "log" |> size_of_file;
          index_log_async = store_path / "index" / "log_async" |> size_of_file;
          store_dict = store_path / "store.dict" |> size_of_file;
          store_pack = store_path / "store.pack" |> size_of_file;
        }

    let rusage () =
      let Rusage.
            {
              utime;
              stime;
              maxrss;
              minflt;
              majflt;
              inblock;
              oublock;
              nvcsw;
              nivcsw;
              _;
            } =
        Rusage.(get Self)
      in
      let maxrss = if is_darwin then Int64.div maxrss 1000L else maxrss in
      Def.
        {utime; stime; maxrss; minflt; majflt; inblock; oublock; nvcsw; nivcsw}

    let now () =
      Mtime_clock.now () |> Mtime.to_uint64_ns |> Int64.to_float
      |> ( *. ) Mtime.ns_to_s

    let create store_path prev_merge_durations =
      Def.
        {
          pack = pack ();
          tree = tree ();
          index = index prev_merge_durations;
          gc = gc ();
          disk = disk store_path;
          rusage = rusage ();
          timestamp_wall = now ();
          timestamp_cpu = Sys.time ();
        }
  end

  let dummy_store_after =
    Def.{watched_nodes_length = List.map (fun _ -> Float.nan) watched_nodes}

  let dummy_store_before =
    Def.{nodes = 0; leafs = 0; skips = 0; depth = 0; width = 0}

  let create_store_before context =
    let+ Impl.{nodes; leafs; skips; depth; width} =
      (* This call directly targets [Impl], it will not be recorded *)
      Impl.stats context
    in
    Def.{nodes; leafs; skips; depth; width}

  let create_store_after context =
    let* watched_nodes_length =
      Lwt_list.map_s
        (fun (_, steps) ->
          (* This call directly targets [Impl], it will not be recorded. *)
          Impl.length context steps >|= function
          | None -> Float.nan
          | Some v -> Float.of_int v)
        Def.step_list_per_watched_node
    in
    Lwt.return Def.{watched_nodes_length}

  module Direct_timer = struct
    let begin_ t = t.direct_timer <- Mtime_clock.counter ()

    let end_ {direct_timer; _} = Mtime_clock.count direct_timer
  end

  module Recursive_timer = struct
    let begin_ t =
      Direct_timer.begin_ t ;
      t.recursive_timers <- Mtime.Span.zero :: t.recursive_timers

    let exit ({recursive_timers; _} as t) =
      match recursive_timers with
      | [] -> assert false
      | hd :: tl ->
          t.recursive_timers <- Mtime.Span.add hd (Direct_timer.end_ t) :: tl

    let enter = Direct_timer.begin_

    let end_ ({recursive_timers; _} as t) =
      match recursive_timers with
      | [] -> assert false
      | hd :: tl ->
          t.recursive_timers <- tl ;
          Mtime.Span.add hd (Direct_timer.end_ t)
  end

  let create_file : string -> Def.config -> string -> t =
   fun path config store_path ->
    let header =
      Def.
        {
          config;
          hostname = Unix.gethostname ();
          word_size = Sys.word_size;
          os_type = Sys.os_type;
          backend_type =
            (match Sys.backend_type with
            | Sys.Native -> `Native
            | Sys.Bytecode -> `Bytecode
            | Sys.Other s -> `Other s);
          big_endian = Sys.big_endian;
          timeofday = Unix.gettimeofday ();
          gc_control = gc_control ();
          runtime_parameters = Sys.runtime_parameters ();
          ocaml_version = Sys.ocaml_version;
          initial_stats =
            Bag_of_stats.create
              store_path
              Index.Stats.((get ()).merge_durations);
        }
    in
    {
      writer = Def.create_file path header;
      store_path;
      direct_timer = Mtime_clock.counter ();
      recursive_timers = [];
      prev_merge_durations = Index.Stats.((get ()).merge_durations);
      bag_before = (* dummy value *) header.initial_stats;
      store_before = dummy_store_before;
    }

  let flush {writer; _} = Def.flush writer

  let close {writer; _} = Def.close writer

  let direct_op_begin = Direct_timer.begin_

  let direct_op_end t short_op =
    let short_op = (short_op :> Def.Frequent_op.tag) in
    let duration =
      Direct_timer.end_ t |> Mtime.Span.to_s |> Int32.bits_of_float
    in
    Def.append_row t.writer (`Frequent_op (short_op, duration))

  let recursive_op_begin = Recursive_timer.begin_

  let recursive_op_exit = Recursive_timer.exit

  let recursive_op_enter = Recursive_timer.enter

  let recursive_op_end t short_op =
    let short_op = (short_op :> Def.Frequent_op.tag) in
    let duration =
      Recursive_timer.end_ t |> Mtime.Span.to_s |> Int32.bits_of_float
    in
    Def.append_row t.writer (`Frequent_op (short_op, duration))

  let commit_begin t ?context () =
    assert (List.length t.recursive_timers = 0) ;
    let stats_before =
      Bag_of_stats.create t.store_path t.prev_merge_durations
    in
    t.prev_merge_durations <- Index.Stats.((get ()).merge_durations) ;
    let+ store_before =
      match context with
      | None -> Lwt.return @@ dummy_store_before
      | Some context -> create_store_before context
    in
    Recursive_timer.begin_ t ;
    t.bag_before <- stats_before ;
    t.store_before <- store_before

  let patch_context_begin = Recursive_timer.exit

  let patch_context_end = Recursive_timer.enter

  let commit_end t ?specs ?context () =
    let duration = Recursive_timer.end_ t in
    let duration = duration |> Mtime.Span.to_s |> Int32.bits_of_float in
    let stats_after = Bag_of_stats.create t.store_path t.prev_merge_durations in
    t.prev_merge_durations <- Index.Stats.((get ()).merge_durations) ;
    let+ store_after =
      match context with
      | None -> Lwt.return @@ dummy_store_after
      | Some context -> create_store_after context
    in
    let op =
      `Commit
        Def.Commit_op.
          {
            duration;
            before = t.bag_before;
            after = stats_after;
            store_before = t.store_before;
            store_after;
            specs;
          }
    in
    Def.append_row t.writer op

  let stats_begin t =
    Direct_timer.begin_ t ;
    let stats_before =
      Bag_of_stats.create t.store_path t.prev_merge_durations
    in
    t.prev_merge_durations <- Index.Stats.((get ()).merge_durations) ;
    t.bag_before <- stats_before

  let stats_end t tag =
    let duration =
      Direct_timer.end_ t |> Mtime.Span.to_s |> Int32.bits_of_float
    in
    let stats_after = Bag_of_stats.create t.store_path t.prev_merge_durations in
    t.prev_merge_durations <- Index.Stats.((get ()).merge_durations) ;
    let op =
      match tag with
      | `Close_op ->
          `Close
            Def.Stats_op.{duration; before = t.bag_before; after = stats_after}
      | `Dump_context_op ->
          `Dump_context
            Def.Stats_op.{duration; before = t.bag_before; after = stats_after}
    in

    Def.append_row t.writer op
end

module Make
    (Impl : Tezos_context_sigs.Context.S) (Trace_config : sig
      val prefix : string

      val message : string option
    end) =
struct
  module Writer = Writer (Impl)

  let path = ref None

  let specs = ref None

  let writer = ref None

  let get_stat_path () =
    match !path with
    | Some path -> path
    | None ->
        Fmt.failwith
          "Trying to get the path from state_trace that hasn't been init yet."

  let set_stat_specs local_specs = specs := Some local_specs

  let setup_writer store_path =
    match !writer with
    | None ->
        let filename = Printf.sprintf "stats_trace.%d.trace" (Unix.getpid ()) in
        let local_path = Misc.prepare_trace_file Trace_config.prefix filename in
        path := Some local_path ;
        Logs.app (fun l -> l "Creating %s" local_path) ;
        let config =
          Def.
            {
              store_type = `Pack;
              setup = `Play ();
              message = Trace_config.message;
            }
        in
        let w = Writer.create_file local_path config store_path in
        writer := Some w
    | Some _ ->
        (* [setup_writer] is not expected to be called several times because
           [init] isn't either. Let's not crash and continue writing into the
           current stats trace. *)
        ()

  let get_writer () =
    match !writer with
    | None -> raise Misc.Stats_trace_without_init
    | Some w -> w

  let () =
    Stdlib.at_exit (fun () ->
        match !writer with None -> () | Some w -> Writer.close w)

  module Impl = Impl

  type varint63 = Optint.Int63.t [@@deriving repr]

  let varint63_t =
    let module V = Repr.Binary.Varint_int63 in
    Repr.like ~bin:(V.encode, V.decode, Obj.magic V.sizer) varint63_t
  (* FIXME: wait for Repr modification to support size in like *)

  type tree = Impl.tree * varint63

  type context = Impl.context * varint63

  let direct_op_begin () = Writer.direct_op_begin (get_writer ())

  let direct_op_end tag = Writer.direct_op_end (get_writer ()) tag

  let close_begin () = Writer.stats_begin (get_writer ())

  let close_end () = Writer.stats_end (get_writer ()) `Close_op

  let dump_context_begin () = Writer.stats_begin (get_writer ())

  let dump_context_end () = Writer.stats_end (get_writer ()) `Dump_context_op

  let recursive_op_begin () = Writer.recursive_op_begin (get_writer ())

  let recursive_op_exit () = Writer.recursive_op_exit (get_writer ())

  let recursive_op_enter () = Writer.recursive_op_enter (get_writer ())

  let recursive_op_end tag = Writer.recursive_op_end (get_writer ()) tag

  module Tree = struct
    let empty _ =
      direct_op_begin () ;
      fun _res -> direct_op_end (`Tree `Empty)

    let of_raw _ =
      direct_op_begin () ;
      fun _res -> direct_op_end (`Tree `Of_raw)

    let of_value _ _ =
      direct_op_begin () ;
      fun _res -> direct_op_end (`Tree `Of_value)

    let mem _ _ =
      direct_op_begin () ;
      fun _res -> direct_op_end (`Tree `Mem)

    let mem_tree _ _ =
      direct_op_begin () ;
      fun _res -> direct_op_end (`Tree `Mem_tree)

    let find _ _ =
      direct_op_begin () ;
      fun _res -> direct_op_end (`Tree `Find)

    let is_empty _ =
      direct_op_begin () ;
      fun _res -> direct_op_end (`Tree `Is_empty)

    let kind _ =
      direct_op_begin () ;
      fun _res -> direct_op_end (`Tree `Kind)

    let hash _ =
      direct_op_begin () ;
      fun _res -> direct_op_end (`Tree `Hash)

    let equal _ _ =
      direct_op_begin () ;
      fun _res -> direct_op_end (`Tree `Equal)

    let to_value _ =
      direct_op_begin () ;
      fun _res -> direct_op_end (`Tree `To_value)

    let clear ~depth:_ _ =
      direct_op_begin () ;
      fun _res -> direct_op_end (`Tree `Clear)

    let find_tree _ _ =
      direct_op_begin () ;
      fun _res -> direct_op_end (`Tree `Find_tree)

    let list _ ~offset:_ ~length:_ =
      direct_op_begin () ;
      fun _res -> direct_op_end (`Tree `List)

    let add _ _ _ =
      direct_op_begin () ;
      fun _res -> direct_op_end (`Tree `Add)

    let add_tree _ _ _ =
      direct_op_begin () ;
      fun _res -> direct_op_end (`Tree `Add_tree)

    let remove _ _ =
      direct_op_begin () ;
      fun _res -> direct_op_end (`Tree `Remove)

    (** Not simple direct *)
    let fold ~depth:_ ~order:_ _ _ =
      recursive_op_begin () ;
      fun _res -> recursive_op_end (`Tree `Fold)

    (** Not simple direct *)
    let fold_step _ _ =
      recursive_op_exit () ;
      fun _res -> recursive_op_enter ()
  end

  let find_tree _ _ =
    direct_op_begin () ;
    fun _res -> direct_op_end `Find_tree

  let list _ ~offset:_ ~length:_ =
    direct_op_begin () ;
    fun _res -> direct_op_end `List

  (** Not simple direct *)
  let fold ~depth:_ ~order:_ _ _ =
    recursive_op_begin () ;
    fun _res -> recursive_op_end (`Tree `Fold)

  (** Not simple direct *)
  let fold_step _ _ =
    recursive_op_exit () ;
    fun _res -> recursive_op_enter ()

  let add_tree _ _ _ =
    direct_op_begin () ;
    fun _res -> direct_op_end `Add_tree

  let mem _ _ =
    direct_op_begin () ;
    fun _res -> direct_op_end `Mem

  let mem_tree _ _ =
    direct_op_begin () ;
    fun _res -> direct_op_end `Mem_tree

  let find _ _ =
    direct_op_begin () ;
    fun _res -> direct_op_end `Find

  let get_protocol _ =
    direct_op_begin () ;
    fun _res -> direct_op_end `Get_protocol

  let hash ~time:_ ~message:_ _ =
    direct_op_begin () ;
    fun _res -> direct_op_end `Hash

  let merkle_tree _ _ _ =
    direct_op_begin () ;
    fun _res -> direct_op_end `Merkle_tree

  let find_predecessor_block_metadata_hash _ =
    direct_op_begin () ;
    fun _res -> direct_op_end `Find_predecessor_block_metadata_hash

  let find_predecessor_ops_metadata_hash _ =
    direct_op_begin () ;
    fun _res -> direct_op_end `Find_predecessor_ops_metadata_hash

  let get_test_chain _ =
    direct_op_begin () ;
    fun _res -> direct_op_end `Get_test_chain

  let exists _ _ =
    direct_op_begin () ;
    fun _res -> direct_op_end `Exists

  let retrieve_commit_info _ _ =
    direct_op_begin () ;
    fun _res -> direct_op_end `Retrieve_commit_info

  let add _ _ _ =
    direct_op_begin () ;
    fun _res -> direct_op_end `Add

  let remove _ _ =
    direct_op_begin () ;
    fun _res -> direct_op_end `Remove

  let add_protocol _ _ =
    direct_op_begin () ;
    fun _res -> direct_op_end `Add_protocol

  let add_predecessor_block_metadata_hash _ _ =
    direct_op_begin () ;
    fun _res -> direct_op_end `Add_predecessor_block_metadata_hash

  let add_predecessor_ops_metadata_hash _ _ =
    direct_op_begin () ;
    fun _res -> direct_op_end `Add_predecessor_ops_metadata_hash

  let add_test_chain _ _ =
    direct_op_begin () ;
    fun _res -> direct_op_end `Add_test_chain

  let remove_test_chain _ =
    direct_op_begin () ;
    fun _res -> direct_op_end `Remove_test_chain

  let fork_test_chain _ ~protocol:_ ~expiration:_ =
    direct_op_begin () ;
    fun _res -> direct_op_end `Fork_test_chain

  let checkout _ _ =
    direct_op_begin () ;
    fun _res ->
      direct_op_end `Checkout ;
      Writer.flush (get_writer ())

  let checkout_exn _ _ =
    direct_op_begin () ;
    fun _res ->
      direct_op_end `Checkout_exn ;
      Writer.flush (get_writer ())

  (** Not simple direct *)
  let close _ =
    close_begin () ;
    fun _res ->
      close_end () ;
      Writer.flush (get_writer ())

  let sync _ =
    direct_op_begin () ;
    fun _res -> direct_op_end `Sync

  let set_master _ _ =
    direct_op_begin () ;
    fun _res -> direct_op_end `Set_master

  let set_head _ _ _ =
    direct_op_begin () ;
    fun _res -> direct_op_end `Set_head

  (** Not simple direct *)
  let commit_genesis _ ~chain_id:_ ~time:_ ~protocol:_ =
    let specs = !specs in
    let* () = Writer.commit_begin (get_writer ()) () in
    Lwt.return @@ fun _res ->
    let* () = Writer.commit_end ?specs (get_writer ()) () in
    Writer.flush (get_writer ()) ;
    Lwt.return_unit

  let clear_test_chain _ _ =
    direct_op_begin () ;
    fun _res -> direct_op_end `Clear_test_chain

  (** Not simple direct *)
  let commit ~time:_ ~message:_ (ctx, _) =
    let specs = !specs in
    let* () = Writer.commit_begin (get_writer ()) ~context:ctx () in
    Lwt.return @@ fun _res ->
    let* () = Writer.commit_end (get_writer ()) ?specs ~context:ctx () in
    Writer.flush (get_writer ()) ;
    Lwt.return_unit

  (** Not simple direct *)
  let commit_test_chain_genesis _ _ =
    direct_op_begin () ;
    fun _res -> direct_op_end `Commit_test_chain_genesis

  (** Not simple direct *)
  let init ~readonly:_ path =
    setup_writer path ;
    direct_op_begin () ;
    fun _res -> direct_op_end `Init

  (** Not simple direct *)
  let patch_context _ (* Writer.patch_context_begin (get_writer ()); *) _res =
    (* Stats_collector.patch_context_end (get_writer ()); *)
    ()

  let restore_context _ ~expected_context_hash:_ ~nb_context_elements:_ ~fd:_ =
    direct_op_begin () ;
    Lwt.return @@ fun _res ->
    direct_op_end `Restore_context ;
    Lwt.return_unit

  let dump_context _ _ ~fd:_ =
    dump_context_begin () ;
    Lwt.return @@ fun _res ->
    dump_context_end () ;
    Lwt.return_unit

  (** Not simple direct *)
  let unhandled _name _res = ()
end
