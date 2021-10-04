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

(** Create a wrapper for a type [t_unwrapped] (i.e. [V.t]) in order to attach
    a tracker to it. The wrapper is of type [t], it contains the tracker.

    The type [t] is abstract so that the type system ensures that all occurences
    of [t_unwrapped] in an API are manually converted to [t].

    The tracker contains a unique id of type [int64] that uniquely identifies
    the wrapped value. It can be used to locate that value inside the
    construction history of all the tracked values.

    For a user of lib_context, there is no semantic difference between a traced
    and an untraceable value. *)
module Make_tracked (V : sig
  type t
end) : sig
  type t

  type t_unwrapped = V.t

  type tracker = Optint.Int63.t

  val wrap : t_unwrapped -> t

  val unwrap : t -> t_unwrapped

  val tracker : t -> tracker
end = struct
  type tracker = Optint.Int63.t

  type t_unwrapped = V.t

  type t = {v : t_unwrapped; tr : tracker}

  let counter = ref (Optint.Int63.of_int64 0L)

  let wrap v =
    let tr = !counter in
    counter := Optint.Int63.succ !counter ;
    {tr; v}

  let unwrap {v; _} = v

  let tracker {tr; _} = tr
end

(** Hide [t_unwrapped] (i.e. [V.t]) under [t] so that the type system ensures that
    all occurences of [t_unwrapped] in an API are manually converted to [t]. *)
module Make_abstract (V : sig
  type t
end) : sig
  type t

  type t_unwrapped = V.t

  val wrap : t_unwrapped -> t

  val unwrap : t -> t_unwrapped
end = struct
  type t_unwrapped = V.t

  type t = V.t

  let wrap = Fun.id

  let unwrap = Fun.id
end

(** Seemlessly wrap [Impl] while notifying the [Recorders] of what's
    happening. *)
module Make
    (Impl : Tezos_context_sigs.Context.S) (Recorders : sig
      module type RECORDER = Recorder.S with module Impl = Impl

      val l : (module RECORDER) list
    end) : Tezos_context_sigs.Context.S = struct
  (** Instanciate the tree tracker *)
  module Tree_traced = Make_tracked (struct
    type t = Impl.tree
  end)

  (** Unpack a wrapped tree in order to forward it to a recorder *)
  let ( !! ) t = (Tree_traced.unwrap t, Tree_traced.tracker t)

  (** Instanciate the context tracker *)
  module Context_traced = Make_tracked (struct
    type t = Impl.context
  end)

  (** Unpack a wrapped context in order to forward it to a recorder *)
  let ( ~~ ) c = (Context_traced.unwrap c, Context_traced.tracker c)

  (** [index] is not tracked. Multiple concurrent [index] on the same process
      would be indistinguishable within a raw actions trace. *)
  module Index_abstract = Make_abstract (struct
    type t = Impl.index
  end)

  let iter_recorders apply_inputs =
    (* First pass the inputs to all recorders (before the call to [Impl]), *)
    let l = List.map apply_inputs Recorders.l in
    fun map_output output ->
      (* then pass the output to all recorders (in reverse order) (after the
         call to [Impl]), *)
      let l = List.rev l in
      let output' = map_output output in
      List.iter (fun f -> f output') l ;
      (* and return the output. *)
      output

  let ( let* ) = ( >>= )

  let iter_recorders_lwt apply_inputs map_output =
    let* l = Lwt_list.map_s apply_inputs Recorders.l in
    Lwt.return @@ fun output ->
    let l = List.rev l in
    let output' = map_output output in
    let* () = Lwt_list.iter_s (fun f -> f output') l in
    Lwt.return output

  let record_unhandled name lwt =
    let record_and_return_output =
      iter_recorders (fun (module R) -> R.unhandled name) Fun.id
    in
    lwt >|= record_and_return_output

  let record_unhandled_direct name f =
    let record_and_return_output =
      iter_recorders (fun (module R) -> R.unhandled name) Fun.id
    in
    f () |> record_and_return_output

  type t = Context_traced.t

  type context = t

  type tree = Tree_traced.t

  type index = Index_abstract.t

  type value = Impl.value

  type key = Impl.key

  type tree_stats = Impl.tree_stats = {
    nodes : int;
    leafs : int;
    skips : int;
    depth : int;
    width : int;
  }

  type module_tree_stats = Impl.module_tree_stats = {
    mutable contents_hash : int;
    mutable contents_find : int;
    mutable contents_add : int;
    mutable contents_mem : int;
    mutable node_hash : int;
    mutable node_mem : int;
    mutable node_index : int;
    mutable node_add : int;
    mutable node_find : int;
    mutable node_val_v : int;
    mutable node_val_find : int;
    mutable node_val_list : int;
  }

  module type S = Tezos_context_sigs.Context.MEM

  type error += Cannot_create_file = Impl.Cannot_create_file

  type error += Cannot_open_file = Impl.Cannot_open_file

  type error += Cannot_find_protocol = Impl.Cannot_find_protocol

  type error += Suspicious_file = Impl.Suspicious_file

  module Tree = struct
    type raw = Impl.Tree.raw

    type repo = Impl.Tree.repo

    (* [_o __ ___] *)

    let empty x =
      let record_and_return_output =
        iter_recorders (fun (module R) -> R.Tree.empty ~~x) (fun res -> !!res)
      in
      Impl.Tree.empty (Context_traced.unwrap x)
      |> Tree_traced.wrap |> record_and_return_output

    let of_raw x =
      let record_and_return_output =
        iter_recorders (fun (module R) -> R.Tree.of_raw x) (fun res -> !!res)
      in
      Impl.Tree.of_raw x |> Tree_traced.wrap |> record_and_return_output

    let of_value x y =
      let record_and_return_output =
        iter_recorders
          (fun (module R) -> R.Tree.of_value ~~x y)
          (fun res -> !!res)
      in
      Impl.Tree.of_value (Context_traced.unwrap x) y >|= fun res ->
      Tree_traced.wrap res |> record_and_return_output

    (* [i_ __ ___] *)

    let mem x y =
      let record_and_return_output =
        iter_recorders (fun (module R) -> R.Tree.mem !!x y) Fun.id
      in
      Impl.Tree.mem (Tree_traced.unwrap x) y >|= record_and_return_output

    let mem_tree x y =
      let record_and_return_output =
        iter_recorders (fun (module R) -> R.Tree.mem_tree !!x y) Fun.id
      in
      Impl.Tree.mem_tree (Tree_traced.unwrap x) y >|= record_and_return_output

    let find x y =
      let record_and_return_output =
        iter_recorders (fun (module R) -> R.Tree.find !!x y) Fun.id
      in
      Impl.Tree.find (Tree_traced.unwrap x) y >|= record_and_return_output

    let is_empty x =
      let record_and_return_output =
        iter_recorders (fun (module R) -> R.Tree.is_empty !!x) Fun.id
      in
      Impl.Tree.is_empty (Tree_traced.unwrap x) |> record_and_return_output

    let kind x =
      let record_and_return_output =
        iter_recorders (fun (module R) -> R.Tree.kind !!x) Fun.id
      in
      Impl.Tree.kind (Tree_traced.unwrap x) |> record_and_return_output

    let hash x =
      let record_and_return_output =
        iter_recorders (fun (module R) -> R.Tree.hash !!x) Fun.id
      in
      Impl.Tree.hash (Tree_traced.unwrap x) |> record_and_return_output

    let equal x y =
      let record_and_return_output =
        iter_recorders (fun (module R) -> R.Tree.equal !!x !!y) Fun.id
      in
      Impl.Tree.equal (Tree_traced.unwrap x) (Tree_traced.unwrap y)
      |> record_and_return_output

    let to_value x =
      let record_and_return_output =
        iter_recorders (fun (module R) -> R.Tree.to_value !!x) Fun.id
      in
      Impl.Tree.to_value (Tree_traced.unwrap x) >|= record_and_return_output

    let clear ?depth x =
      let record_and_return_output =
        iter_recorders (fun (module R) -> R.Tree.clear ~depth !!x) Fun.id
      in
      Impl.Tree.clear ?depth (Tree_traced.unwrap x) |> record_and_return_output

    (* [io __ ___] *)

    let find_tree x y =
      let record_and_return_output =
        iter_recorders
          (fun (module R) -> R.Tree.find_tree !!x y)
          (Option.map ( !! ))
      in
      Impl.Tree.find_tree (Tree_traced.unwrap x) y >|= fun res ->
      Option.map Tree_traced.wrap res |> record_and_return_output

    let list x ?offset ?length y =
      let record_and_return_output =
        iter_recorders
          (fun (module R) -> R.Tree.list !!x ~offset ~length)
          (List.map (fun (step, tree) -> (step, !!tree)))
      in
      Impl.Tree.list (Tree_traced.unwrap x) ?offset ?length y >|= fun l ->
      List.map (fun (a, b) -> (a, Tree_traced.wrap b)) l
      |> record_and_return_output

    let add x y z =
      let record_and_return_output =
        iter_recorders (fun (module R) -> R.Tree.add !!x y z) ( !! )
      in
      Impl.Tree.add (Tree_traced.unwrap x) y z >|= fun res ->
      Tree_traced.wrap res |> record_and_return_output

    let add_tree x y z =
      let record_and_return_output =
        iter_recorders (fun (module R) -> R.Tree.add_tree !!x y !!z) ( !! )
      in
      Impl.Tree.add_tree (Tree_traced.unwrap x) y (Tree_traced.unwrap z)
      >|= fun res -> Tree_traced.wrap res |> record_and_return_output

    let remove x y =
      let record_and_return_output =
        iter_recorders (fun (module R) -> R.Tree.remove !!x y) ( !! )
      in
      Impl.Tree.remove (Tree_traced.unwrap x) y >|= fun res ->
      Tree_traced.wrap res |> record_and_return_output

    let fold ?depth x y ~order ~init ~f =
      let entry_count = ref 0 in
      let f a b c =
        let b = Tree_traced.wrap b in
        let record_and_return_output =
          iter_recorders
            (fun (module R) -> R.Tree.fold_step !entry_count !!b)
            Fun.id
        in
        f a b c >|= fun res ->
        record_and_return_output () ;
        incr entry_count ;
        res
      in
      let record_and_return_output =
        iter_recorders
          (fun (module R) -> R.Tree.fold ~depth ~order !!x y)
          Fun.id
      in
      Impl.Tree.fold ~order ?depth (Tree_traced.unwrap x) y ~init ~f
      >|= fun res ->
      let (_ : int) = record_and_return_output !entry_count in
      res

    (* loosely tracked *)

    let shallow x y =
      record_unhandled_direct Recorder.Tree_shallow @@ fun () ->
      Impl.Tree.shallow x y >|= Tree_traced.wrap

    let to_raw x =
      record_unhandled Recorder.Tree_to_raw
      @@ Impl.Tree.to_raw (Tree_traced.unwrap x)

    let pp x y =
      record_unhandled_direct Recorder.Tree_pp @@ fun () ->
      Impl.Tree.pp x (Tree_traced.unwrap y)

    let length x y =
      record_unhandled Recorder.Tree_length
      @@ Impl.Tree.length (Tree_traced.unwrap x) y

    let stats x =
      record_unhandled Recorder.Tree_stats
      @@ Impl.Tree.stats (Tree_traced.unwrap x)

    (* not tracked and not wrapped *)

    let make_repo = Impl.Tree.make_repo

    let raw_encoding = Impl.Tree.raw_encoding
  end

  (* [_o i_ ___] - The function(s) from context to tree *)

  let find_tree x y =
    let record_and_return_output =
      iter_recorders (fun (module R) -> R.find_tree ~~x y) (Option.map ( !! ))
    in
    Impl.find_tree (Context_traced.unwrap x) y >|= fun res ->
    Option.map Tree_traced.wrap res |> record_and_return_output

  let list x ?offset ?length y =
    let record_and_return_output =
      iter_recorders
        (fun (module R) -> R.list ~~x ~offset ~length)
        (List.map (fun (step, tree) -> (step, !!tree)))
    in
    Impl.list (Context_traced.unwrap x) ?offset ?length y >|= fun l ->
    List.map (fun (a, b) -> (a, Tree_traced.wrap b)) l
    |> record_and_return_output

  let fold ?depth x y ~order ~init ~f =
    let entry_count = ref 0 in
    let f a b c =
      let b = Tree_traced.wrap b in
      let record_and_return_output =
        iter_recorders (fun (module R) -> R.fold_step !entry_count !!b) Fun.id
      in
      f a b c >|= fun res ->
      record_and_return_output () ;
      incr entry_count ;
      res
    in
    let record_and_return_output =
      iter_recorders (fun (module R) -> R.fold ~depth ~order ~~x y) Fun.id
    in
    Impl.fold ~order ?depth (Context_traced.unwrap x) y ~init ~f >|= fun res ->
    let (_ : int) = record_and_return_output !entry_count in
    res

  (* [i_ io ___] - The function(s) from tree to context*)

  let add_tree x y z =
    let record_and_return_output =
      iter_recorders (fun (module R) -> R.add_tree ~~x y !!z) ( ~~ )
    in
    Impl.add_tree (Context_traced.unwrap x) y (Tree_traced.unwrap z)
    >|= fun res -> Context_traced.wrap res |> record_and_return_output

  (* [__ i_ ___] *)

  let mem x y =
    let record_and_return_output =
      iter_recorders (fun (module R) -> R.mem ~~x y) Fun.id
    in
    Impl.mem (Context_traced.unwrap x) y >|= record_and_return_output

  let mem_tree x y =
    let record_and_return_output =
      iter_recorders (fun (module R) -> R.mem_tree ~~x y) Fun.id
    in
    Impl.mem_tree (Context_traced.unwrap x) y >|= record_and_return_output

  let find x y =
    let record_and_return_output =
      iter_recorders (fun (module R) -> R.find ~~x y) Fun.id
    in
    Impl.find (Context_traced.unwrap x) y >|= record_and_return_output

  let get_protocol x =
    let record_and_return_output =
      iter_recorders (fun (module R) -> R.get_protocol ~~x) Fun.id
    in
    Impl.get_protocol (Context_traced.unwrap x) >|= record_and_return_output

  let hash ~time ?message x =
    let record_and_return_output =
      iter_recorders (fun (module R) -> R.hash ~time ~message ~~x) Fun.id
    in
    Impl.hash ~time ?message (Context_traced.unwrap x)
    |> record_and_return_output

  let merkle_tree x y z =
    let record_and_return_output =
      iter_recorders (fun (module R) -> R.merkle_tree ~~x y z) Fun.id
    in
    Impl.merkle_tree (Context_traced.unwrap x) y z >|= record_and_return_output

  let find_predecessor_block_metadata_hash x =
    let record_and_return_output =
      iter_recorders
        (fun (module R) -> R.find_predecessor_block_metadata_hash ~~x)
        Fun.id
    in
    Impl.find_predecessor_block_metadata_hash (Context_traced.unwrap x)
    >|= record_and_return_output

  let find_predecessor_ops_metadata_hash x =
    let record_and_return_output =
      iter_recorders
        (fun (module R) -> R.find_predecessor_ops_metadata_hash ~~x)
        Fun.id
    in
    Impl.find_predecessor_ops_metadata_hash (Context_traced.unwrap x)
    >|= record_and_return_output

  let get_test_chain x =
    let record_and_return_output =
      iter_recorders (fun (module R) -> R.get_test_chain ~~x) Fun.id
    in
    Impl.get_test_chain (Context_traced.unwrap x) >|= record_and_return_output

  (* [__ __ i__] *)

  let exists x y =
    let x = Index_abstract.unwrap x in
    let record_and_return_output =
      iter_recorders (fun (module R) -> R.exists x y) Fun.id
    in
    Impl.exists x y >|= record_and_return_output

  let retrieve_commit_info x y =
    let x = Index_abstract.unwrap x in
    let record_and_return_output =
      iter_recorders (fun (module R) -> R.retrieve_commit_info x y) Fun.id
    in
    Impl.retrieve_commit_info x y >|= record_and_return_output

  (* [__ io ___] *)

  let add x y z =
    let record_and_return_output =
      iter_recorders (fun (module R) -> R.add ~~x y z) ( ~~ )
    in
    Impl.add (Context_traced.unwrap x) y z >|= fun res ->
    Context_traced.wrap res |> record_and_return_output

  let remove x y =
    let record_and_return_output =
      iter_recorders (fun (module R) -> R.remove ~~x y) ( ~~ )
    in
    Impl.remove (Context_traced.unwrap x) y >|= fun res ->
    Context_traced.wrap res |> record_and_return_output

  let add_protocol x y =
    let record_and_return_output =
      iter_recorders (fun (module R) -> R.add_protocol ~~x y) ( ~~ )
    in
    Impl.add_protocol (Context_traced.unwrap x) y >|= fun res ->
    Context_traced.wrap res |> record_and_return_output

  let add_predecessor_block_metadata_hash x y =
    let record_and_return_output =
      iter_recorders
        (fun (module R) -> R.add_predecessor_block_metadata_hash ~~x y)
        ( ~~ )
    in
    Impl.add_predecessor_block_metadata_hash (Context_traced.unwrap x) y
    >|= fun res -> Context_traced.wrap res |> record_and_return_output

  let add_predecessor_ops_metadata_hash x y =
    let record_and_return_output =
      iter_recorders
        (fun (module R) -> R.add_predecessor_ops_metadata_hash ~~x y)
        ( ~~ )
    in
    Impl.add_predecessor_ops_metadata_hash (Context_traced.unwrap x) y
    >|= fun res -> Context_traced.wrap res |> record_and_return_output

  let add_test_chain x y =
    let record_and_return_output =
      iter_recorders (fun (module R) -> R.add_test_chain ~~x y) ( ~~ )
    in
    Impl.add_test_chain (Context_traced.unwrap x) y >|= fun res ->
    Context_traced.wrap res |> record_and_return_output

  let remove_test_chain x =
    let record_and_return_output =
      iter_recorders (fun (module R) -> R.remove_test_chain ~~x) ( ~~ )
    in
    Impl.remove_test_chain (Context_traced.unwrap x) >|= fun res ->
    Context_traced.wrap res |> record_and_return_output

  let fork_test_chain x ~protocol ~expiration =
    let record_and_return_output =
      iter_recorders
        (fun (module R) -> R.fork_test_chain ~~x ~protocol ~expiration)
        ( ~~ )
    in
    Impl.fork_test_chain (Context_traced.unwrap x) ~protocol ~expiration
    >|= fun res -> Context_traced.wrap res |> record_and_return_output

  (* [__ _o i__] *)

  let checkout x y =
    let x = Index_abstract.unwrap x in
    let record_and_return_output =
      iter_recorders (fun (module R) -> R.checkout x y) (Option.map ( ~~ ))
    in
    Impl.checkout x y >|= fun res ->
    Option.map Context_traced.wrap res |> record_and_return_output

  let checkout_exn x y =
    let x = Index_abstract.unwrap x in
    let record_and_return_output =
      iter_recorders
        (fun (module R) -> R.checkout_exn x y)
        (function Ok res -> Ok ~~res | Error _ as e -> e)
    in
    Lwt.try_bind
      (fun () -> Impl.checkout_exn x y)
      (fun res -> Ok (Context_traced.wrap res) |> Lwt.return)
      (fun exn -> Error exn |> Lwt.return)
    >|= fun res ->
    let (_ : _ result) = record_and_return_output res in
    match res with Error exn -> raise exn | Ok v -> v

  (* [__ __ i_m] *)

  let close x =
    let x = Index_abstract.unwrap x in
    let record_and_return_output =
      iter_recorders (fun (module R) -> R.close x) Fun.id
    in
    Impl.close x >|= record_and_return_output

  let sync x =
    let x = Index_abstract.unwrap x in
    let record_and_return_output =
      iter_recorders (fun (module R) -> R.sync x) Fun.id
    in
    Impl.sync x >|= record_and_return_output

  let set_master x y =
    let x = Index_abstract.unwrap x in
    let record_and_return_output =
      iter_recorders (fun (module R) -> R.set_master x y) Fun.id
    in
    Impl.set_master x y >|= record_and_return_output

  let set_head x y z =
    let x = Index_abstract.unwrap x in
    let record_and_return_output =
      iter_recorders (fun (module R) -> R.set_head x y z) Fun.id
    in
    Impl.set_head x y z >|= record_and_return_output

  let commit_genesis x ~chain_id ~time ~protocol =
    let x = Index_abstract.unwrap x in
    let* record_and_return_output =
      iter_recorders_lwt
        (fun (module R) -> R.commit_genesis x ~chain_id ~time ~protocol)
        Fun.id
    in
    Impl.commit_genesis x ~chain_id ~time ~protocol >>= record_and_return_output

  let clear_test_chain x y =
    let x = Index_abstract.unwrap x in
    let record_and_return_output =
      iter_recorders (fun (module R) -> R.clear_test_chain x y) Fun.id
    in
    Impl.clear_test_chain x y >|= record_and_return_output

  (* [__ i_ __m] *)

  let commit ~time ?message x =
    let* record_and_return_output =
      iter_recorders_lwt (fun (module R) -> R.commit ~time ~message ~~x) Fun.id
    in
    Impl.commit ~time ?message (Context_traced.unwrap x)
    >>= record_and_return_output

  let commit_test_chain_genesis x y =
    let record_and_return_output =
      iter_recorders
        (fun (module R) -> R.commit_test_chain_genesis ~~x y)
        Fun.id
    in
    Impl.commit_test_chain_genesis (Context_traced.unwrap x) y
    >|= record_and_return_output

  (* [__ ~~ _o_] *)

  let init ?patch_context:user_patch_context_opt ?readonly ?indexing_strategy x
      =
    let create_local_patch_context user_patch_context ctx =
      let ctx = Context_traced.wrap ctx in
      let record_and_return_output =
        iter_recorders
          (fun (module R) -> R.patch_context ~~ctx)
          (function Ok res -> Ok ~~res | Error _ as e -> e)
      in
      user_patch_context ctx >|= fun res ->
      record_and_return_output res |> Result.map Context_traced.unwrap
    in
    let local_patch_context_opt =
      Option.map create_local_patch_context user_patch_context_opt
    in
    let record_and_return_output =
      iter_recorders
        (fun (module R) ->
          R.init ~readonly (* TODO: maiste (~indexing_strategy) *) x)
        Fun.id
    in
    Impl.init
      ?patch_context:local_patch_context_opt
      ?readonly
      ?indexing_strategy
      x
    >|= fun res -> record_and_return_output res |> Index_abstract.wrap

  (* loosely tracked *)

  let length x y =
    record_unhandled Recorder.Length @@ Impl.length (Context_traced.unwrap x) y

  let stats x =
    record_unhandled Recorder.Stats @@ Impl.stats (Context_traced.unwrap x)

  let restore_context x ~expected_context_hash ~nb_context_elements ~fd =
    let x = Index_abstract.unwrap x in
    let* record_and_return_output =
      iter_recorders_lwt
        (fun (module R) ->
          R.restore_context ~expected_context_hash ~nb_context_elements ~fd x)
        Fun.id
    in
    Impl.restore_context ~expected_context_hash ~nb_context_elements ~fd x
    >>= record_and_return_output

  let dump_context x y ~fd =
    let x = Index_abstract.unwrap x in
    let* record_and_return_output =
      iter_recorders_lwt (fun (module R) -> R.dump_context x y ~fd) Fun.id
    in
    Impl.dump_context x y ~fd >>= record_and_return_output

  let check_protocol_commit_consistency ~expected_context_hash
      ~given_protocol_hash ~author ~message ~timestamp ~test_chain_status
      ~predecessor_block_metadata_hash ~predecessor_ops_metadata_hash
      ~data_merkle_root ~parents_contexts =
    record_unhandled Recorder.Check_protocol_commit_consistency
    @@ Impl.check_protocol_commit_consistency
         ~expected_context_hash
         ~given_protocol_hash
         ~author
         ~message
         ~timestamp
         ~test_chain_status
         ~predecessor_block_metadata_hash
         ~predecessor_ops_metadata_hash
         ~data_merkle_root
         ~parents_contexts

  (* not tracked and not wrapped *)

  let module_tree_stats = Impl.module_tree_stats

  let compute_testchain_genesis = Impl.compute_testchain_genesis

  let compute_testchain_chain_id = Impl.compute_testchain_chain_id

  module Checks = Impl.Checks

  let get_hash_version _ = assert false

  let set_hash_version _ = assert false
end
