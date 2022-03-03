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

module Def = Raw_actions_trace_definition

(** [system_wide_now] will be used to estimate the ordering of the [commit] and
    [sync] calls from one process to another.

    Mtime relies on CLOCK_MONOTONIC.

    https://stackoverflow.com/a/3527632 states:
    If you want to compute the elapsed time between two events observed on
    the one machine without an intervening reboot, CLOCK_MONOTONIC is the
    best option.
*)
let system_wide_now () =
  Mtime_clock.now () |> Mtime.to_uint64_ns |> Int64.to_float |> ( *. ) 1e-9

module Make
    (Impl : Tezos_context_sigs.Context.MACHIN) (Trace_config : sig
      val prefix : string
    end) =
struct
  (** Per-process raw trace writer. *)
  let writer = ref None

  let setup_writer () =
    match !writer with
    | None ->
        let filename =
          Printf.sprintf "raw_actions_trace.%d.trace" (Unix.getpid ())
        in
        let path = Misc.prepare_trace_file Trace_config.prefix filename in
        Logs.app (fun l -> l "Creating %s" path) ;
        writer := Some (Def.create_file path ())
    | Some _ -> ()
  (* This function is not expected to be called several times because
     the [init] function must be called once. *)

  let get_writer () =
    match !writer with
    | None -> raise Misc.Raw_trace_without_init
    | Some writer -> writer

  let () =
    Stdlib.at_exit (fun () ->
        match !writer with None -> () | Some w -> Def.close w)

  module Impl = Impl

  type varint63 = Optint.Int63.t [@@deriving repr]

  let varint63_t =
    let module V = Repr.Binary.Varint_int63 in
    Repr.like ~bin:(V.encode, V.decode, Obj.magic V.sizer) varint63_t
  (* FIXME: wait for Repr modification to support size in like *)

  type tree = Impl.tree * varint63

  type context = Impl.context * varint63

  (** Write a new [row] to [writer] *)
  let push v = Def.append_row (get_writer ()) v

  let encode_output_value b =
    if Bytes.length b <= 17 then `Value b
    else `Hash (Digestif.MD5.digestv_bytes [b])

  module Tree = struct
    (** Write a new [row] to [writer] *)
    let push v = Def.append_row (get_writer ()) (Def.Tree v)

    let empty (_, x) (_, res) = Def.Tree.Empty (x, res) |> push

    let of_value (_, x) v (_, res) = Def.Tree.Of_value ((x, v), res) |> push

    let of_raw raw (_, res) =
      let rec aux = function
        | `Value v -> `Value v
        | `Tree map ->
            `Tree
              (String.Map.bindings map
              |> List.map (fun (step, raw) -> (step, aux raw)))
      in
      Def.Tree.Of_raw (aux raw, res) |> push

    let mem (_, x) y res = Def.Tree.Mem ((x, y), res) |> push

    let mem_tree (_, x) y res = Def.Tree.Mem_tree ((x, y), res) |> push

    let find (_, x) y res =
      let res = Option.map encode_output_value res in
      Def.Tree.Find ((x, y), res) |> push

    let is_empty (_, x) res = Def.Tree.Is_empty (x, res) |> push

    let kind (_, x) res = Def.Tree.Kind (x, res) |> push

    let hash (_, x) res =
      let res = Context_hash.to_string res in
      Def.Tree.Hash (x, res) |> push

    let equal (_, x) (_, y) res = Def.Tree.Equal ((x, y), res) |> push

    let to_value (_, x) res =
      let res = Option.map encode_output_value res in
      Def.Tree.To_value (x, res) |> push

    let clear ~depth (_, x) () = Def.Tree.Clear ((depth, x), ()) |> push

    let find_tree (_, x) y res =
      let res = Option.map (fun (_, res) -> res) res in
      Def.Tree.Find_tree ((x, y), res) |> push

    let list (_, x) ~offset ~length res =
      let count = List.length res in
      let first_tracker =
        match List.nth_opt res 0 with
        | None -> Optint.Int63.of_int64 (-42L)
        | Some (_, (_, first)) -> first
      in
      let res = Def.{first_tracker; count} in
      Def.Tree.List ((x, offset, length), res) |> push

    let add (_, x) y z (_, res) = Def.Tree.Add ((x, y, z), res) |> push

    let add_tree (_, x) y (_, z) (_, res) =
      Def.Tree.Add_tree ((x, y, z), res) |> push

    let remove (_, x) y (_, res) = Def.Tree.Remove ((x, y), res) |> push

    let fold ~depth ~order (_, x) y =
      Def.Tree.Fold_start ((depth, order), x, y) |> push ;
      fun z -> Def.Tree.Fold_end z |> push

    let fold_step _i (_, y) =
      Def.Tree.Fold_step_enter y |> push ;
      fun () -> Def.Tree.Fold_step_exit y |> push
  end

  let find_tree (_, x) y res =
    let res = Option.map (fun (_, res) -> res) res in
    Def.Find_tree ((x, y), res) |> push

  let list (_, x) ~offset ~length res =
    let count = List.length res in
    let first_tracker =
      match List.nth_opt res 0 with
      | None -> Optint.Int63.of_int64 (-42L)
      | Some (_, (_, first)) -> first
    in
    let res = Def.{first_tracker; count} in
    Def.List ((x, offset, length), res) |> push

  let fold ~depth ~order (_, x) y =
    Def.Fold_start ((depth, order), x, y) |> push ;
    fun z -> Def.Fold_end z |> push

  let fold_step _i (_, y) =
    Def.Fold_step_enter y |> push ;
    fun () -> Def.Fold_step_exit y |> push

  let add_tree (_, x) y (_, z) (_, res) = Def.Add_tree ((x, y, z), res) |> push

  let mem (_, x) y res = Def.Mem ((x, y), res) |> push

  let mem_tree (_, x) y res = Def.Mem_tree ((x, y), res) |> push

  let find (_, x) y res =
    let res = Option.map encode_output_value res in
    Def.Find ((x, y), res) |> push

  let get_protocol (_, x) res =
    let res = Protocol_hash.to_string res in
    Def.Get_protocol (x, res) |> push

  let hash ~time ~message (_, x) res =
    let time = Time.Protocol.to_seconds time in
    let res = Context_hash.to_string res in
    Def.Hash ((time, message, x), res) |> push

  let merkle_tree (_, x) y z _res = Def.Merkle_tree ((x, y, z), ()) |> push

  let find_predecessor_block_metadata_hash (_, x) res =
    let res = Option.map Block_metadata_hash.to_string res in
    Def.Find_predecessor_block_metadata_hash (x, res) |> push

  let find_predecessor_ops_metadata_hash (_, x) res =
    let res = Option.map Operation_metadata_list_list_hash.to_string res in
    Def.Find_predecessor_ops_metadata_hash (x, res) |> push

  let get_test_chain (_, x) res = Def.Get_test_chain (x, res) |> push

  let exists _index x =
    let x = Context_hash.to_string x in
    let before = system_wide_now () in
    fun res ->
      let after = system_wide_now () in
      Def.Exists ((x, res), {before; after}) |> push

  let retrieve_commit_info _index x =
    let x = Context_hash.to_string x.Block_header.shell.context in
    let before = system_wide_now () in
    fun res ->
      let after = system_wide_now () in
      Def.Retrieve_commit_info ((x, Result.is_ok res), {before; after}) |> push

  let add (_, x) y z (_, res) = Def.Add ((x, y, z), res) |> push

  let remove (_, x) y (_, res) = Def.Remove ((x, y), res) |> push

  let add_protocol (_, x) y (_, res) =
    let y = Protocol_hash.to_string y in
    Def.Add_protocol ((x, y), res) |> push

  let add_predecessor_block_metadata_hash (_, x) y (_, res) =
    let y = Block_metadata_hash.to_string y in
    Def.Add_predecessor_block_metadata_hash ((x, y), res) |> push

  let add_predecessor_ops_metadata_hash (_, x) y (_, res) =
    let y = Operation_metadata_list_list_hash.to_string y in
    Def.Add_predecessor_ops_metadata_hash ((x, y), res) |> push

  let add_test_chain (_, x) y (_, res) =
    Def.Add_test_chain ((x, y), res) |> push

  let remove_test_chain (_, x) (_, res) = Def.Remove_test_chain (x, res) |> push

  let fork_test_chain (_, x) ~protocol ~expiration (_, res) =
    let protocol = Protocol_hash.to_string protocol in
    let expiration = Time.Protocol.to_seconds expiration in
    Def.Fork_test_chain ((x, protocol, expiration), res) |> push

  let checkout _index x =
    let x = Context_hash.to_string x in
    let before = system_wide_now () in
    fun res ->
      let after = system_wide_now () in
      let res = Option.map (fun (_, res) -> res) res in
      Def.Checkout ((x, res), {before; after}) |> push ;
      (* Flush trace on checkout *)
      Def.flush (get_writer ())

  let checkout_exn _index x =
    let x = Context_hash.to_string x in
    let before = system_wide_now () in
    fun res ->
      let after = system_wide_now () in
      let res = match res with Error _ -> Error () | Ok (_, res) -> Ok res in
      Def.Checkout_exn ((x, res), {before; after}) |> push ;
      (* Flush trace on checkout *)
      Def.flush (get_writer ())

  let close _index () =
    Def.Close |> push ;
    (* Flush trace on close *)
    Def.flush (get_writer ())

  let sync _index =
    let before = system_wide_now () in
    fun () ->
      let after = system_wide_now () in
      Def.Sync {before; after} |> push

  let set_master _index x () =
    let x = Context_hash.to_string x in
    Def.Set_master (x, ()) |> push

  let set_head _index x y () =
    let x = Chain_id.to_string x in
    let y = Context_hash.to_string y in
    Def.Set_head ((x, y), ()) |> push

  let commit_genesis _index ~chain_id ~time ~protocol =
    let chain_id = Chain_id.to_string chain_id in
    let time = Time.Protocol.to_seconds time in
    let protocol = Protocol_hash.to_string protocol in
    let before = system_wide_now () in
    Def.Commit_genesis_start (((chain_id, time, protocol), ()), before) |> push ;
    Lwt.return @@ fun res ->
    let after = system_wide_now () in
    let res =
      match res with
      | Error _ -> Error ()
      | Ok res -> Ok (Context_hash.to_string res)
    in
    Def.Commit_genesis_end (((), res), after) |> push ;
    Lwt.return_unit

  let clear_test_chain _index x () =
    let x = Chain_id.to_string x in
    Def.Clear_test_chain (x, ()) |> push

  let commit ~time ~message (_, x) =
    let time = Time.Protocol.to_seconds time in
    let before = system_wide_now () in
    Lwt.return @@ fun res ->
    let after = system_wide_now () in
    let res = Context_hash.to_string res in
    Def.Commit (((time, message, x), res), {before; after}) |> push ;
    Lwt.return_unit

  let commit_test_chain_genesis (_, x) y =
    let before = system_wide_now () in
    fun _res ->
      let after = system_wide_now () in
      let y =
        Block_header.(
          let a = Time.Protocol.to_seconds y.shell.timestamp in
          let b = y.shell.level in
          let c = Context_hash.to_string y.shell.context in
          (a, b, c))
      in
      Def.Commit_test_chain_genesis (((x, y), ()), {before; after}) |> push

  let init ~readonly _path _res =
    let readonly = match readonly with Some true -> true | _ -> false in
    setup_writer () ;
    Def.Init (readonly, ()) |> push

  let patch_context (_, x) =
    Def.Patch_context_enter x |> push ;
    fun res ->
      let res = match res with Error _ -> Error () | Ok (_, res) -> Ok res in
      Def.Patch_context_exit (x, res) |> push

  let restore_context _ ~expected_context_hash:_ ~nb_context_elements:_ ~fd:_
      ~legacy:_ ~in_memory:_ ~progress_display_mode:_ =
    Lwt.return @@ fun _res ->
    Def.Unhandled Recorder.Restore_context |> push ;
    Lwt.return_unit

  let dump_context _ _ ~fd:_ ~on_disk:_ ~progress_display_mode:_ =
    let before = system_wide_now () in
    Lwt.return @@ fun _res ->
    let after = system_wide_now () in
    Def.Dump_context {before; after} |> push ;
    Lwt.return_unit

  let unhandled name _res = Def.Unhandled name |> push
end
