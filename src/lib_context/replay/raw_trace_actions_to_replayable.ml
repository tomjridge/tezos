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

(** Conversion from a raw to a replayable actions trace.

    It may crack your memory.

    Main tasks:

    - Filter out some of the unnecessary events
    - Guess the missing block levels
    - Group the events by block
    - Annotate the tracker ids on trees, context and commits with scope info
    - Summarise the content of the trace to the header
    - Act on a slice of the raw trace (NYI) *)

open Lwt.Syntax
module Def0 = Tezos_context_recording.Raw_actions_trace_definition
module Def1 = Replay_actions_trace_definitions
module Trace_common = Tezos_context_recording.Trace_common
module Seq = Trace_common.Seq
module Hashtbl = Stdlib.Hashtbl
module List = Stdlib.List

let convolve_with_padding :
    ('a option -> 'a -> 'a option -> 'b) -> 'a list -> 'b list =
 fun f l ->
  let a = Array.of_list l in
  let count = Array.length a in
  List.init (Array.length a) (fun i ->
      let prev = if i = 0 then None else Some a.(i - 1) in
      let next = if i = count - 1 then None else Some a.(i + 1) in
      f prev a.(i) next)

let iter_2by2 : ('a -> 'a -> unit) -> 'a list -> unit =
 fun f l ->
  let a = Array.of_list l in
  let count = Array.length a in
  for i = 0 to count - 2 do
    f a.(i) a.(i + 1)
  done

let level_of_commit_message message =
  match
    String.split_on_char ',' message
    |> List.map String.trim
    |> List.map (String.split_on_char ' ')
  with
  | [["lvl"; lvl]; ["fit"; _]; ["prio"; _]; [_; "ops"]] -> int_of_string lvl
  | _ -> Fmt.failwith "Could not parse commit message: `%s`" message

module Slice_input_seq = struct
  type acc = {
    seq : Def0.row Seq.t;
    looking_for_first : bool;
    commits_seen : int;
    commits_sent : int;
    rev_events_of_current_block : Def0.row list;
  }

  let block_level_opt_of_raw_row = function
    | Def0.Commit (((_, message_opt, _), _), _) ->
        Option.map level_of_commit_message message_opt
    | Def0.Commit_genesis_end _ -> Some 0
    | _ -> None

  let slice ~first ~last seq =
    let is_first_commit acc lvl_opt =
      assert acc.looking_for_first ;
      match (first, lvl_opt) with
      | (`Idx i, _) when i < acc.commits_seen ->
          Fmt.failwith "First block idx should be greater than 0"
      | (`Idx i, _) when i = acc.commits_seen -> true
      | (`Idx _, _) -> false
      | (`Level _, None) -> false
      | (`Level i, Some j) when i > j -> false
      | (`Level i, Some j) when i = j -> true
      | (`Level _, Some _) ->
          Fmt.failwith "Could not find requested first block level in input seq"
    in
    let is_last_commit acc lvl_opt =
      match (last, lvl_opt) with
      | (`End, _) -> false
      | (`Count n, _) when n > acc.commits_sent + 1 -> false
      | (`Count n, _) when n = acc.commits_sent + 1 -> true
      | (`Count _, _) ->
          Fmt.failwith "The number of requested blocks should be greater than 0"
      | (`Level _, None) -> false
      | (`Level i, Some j) when i > j -> false
      | (`Level i, Some j) when i = j -> true
      | (`Level _, Some _) ->
          Fmt.failwith "Could not find requested last block level in input seq"
    in
    let rec aux acc =
      match acc.seq () with
      | Seq.Nil -> (
          if acc.looking_for_first then
            Fmt.failwith "Reached end of input seq before first commit"
          else
            match last with
            | `End -> Seq.Nil
            | `Count _ | `Level _ ->
                Fmt.failwith
                  "Reached end of input seq without finding requested last")
      | Seq.Cons (ev, rest) -> (
          let rev_events_of_current_block =
            ev :: acc.rev_events_of_current_block
          in
          match ev with
          | Def0.Commit _ | Commit_genesis_end _ ->
              let lvl_opt = block_level_opt_of_raw_row ev in
              let (is_first, is_included, is_last) =
                if acc.looking_for_first then
                  let is_first = is_first_commit acc lvl_opt in
                  (is_first, is_first, is_last_commit acc lvl_opt)
                else (false, true, is_last_commit acc lvl_opt)
              in
              let acc =
                {
                  commits_seen = acc.commits_seen + 1;
                  seq = rest;
                  looking_for_first =
                    (if is_first then false else acc.looking_for_first);
                  rev_events_of_current_block = [];
                  commits_sent =
                    (if is_included then acc.commits_sent + 1
                    else acc.commits_sent);
                }
              in
              if is_included then
                yield_events_of_block
                  (List.rev rev_events_of_current_block)
                  (if is_last then Seq.empty else fun () -> aux acc)
              else aux acc
          | _ -> aux {acc with seq = rest; rev_events_of_current_block})
    and yield_events_of_block evs k =
      match evs with
      | [] -> k ()
      | hd :: tl -> Seq.Cons (hd, fun () -> yield_events_of_block tl k)
    in
    fun () ->
      aux
        {
          seq;
          looking_for_first = true;
          commits_seen = 0;
          commits_sent = 0;
          rev_events_of_current_block = [];
        }
end

type hash = Def0.hash

(** The 3 massive hash tables that will contain all the tracker occurences from
    the trace. *)
type tracker_id_tables = {
  occurrence_count_per_tree_id : (Def0.tracker, int) Hashtbl.t;
  occurrence_count_per_context_id : (Def0.tracker, int) Hashtbl.t;
  occurrence_count_per_commit_hash : (string, int * int) Hashtbl.t;
}

(** A summary of one [Def0.row] *)
type event_details = {
  rank : [`Ignore | `Crash | `Use | `Control_flow];
  tracker_ids : Def0.tracker list * Def0.tracker list * Def0.commit_hash list;
  to_v1 : tracker_id_tables -> Def1.event;
}

(** A single large multi-purpose pattern matching to filter/inspect/map the raw
    rows. *)
let event_infos =
  let ignore_ =
    {
      rank = `Ignore;
      tracker_ids = ([], [], []);
      to_v1 = (fun _ -> assert false);
    }
  in
  let crash =
    {rank = `Crash; tracker_ids = ([], [], []); to_v1 = (fun _ -> assert false)}
  in
  let to_tree t = Def1.Tree t in

  let (scopec, scopet) =
    let add_scope tbl k =
      match Hashtbl.find_opt tbl k with
      | Some 0 | None -> Fmt.failwith "Unexpected scope"
      | Some 1 ->
          Hashtbl.remove tbl k ;
          (Def1.Last_occurence, k)
      | Some i ->
          Hashtbl.replace tbl k (i - 1) ;
          (Def1.Will_reoccur, k)
    in
    ( (fun ids -> add_scope ids.occurrence_count_per_context_id),
      fun ids -> add_scope ids.occurrence_count_per_tree_id )
  in
  let scopeh_rhs ids k =
    let tbl = ids.occurrence_count_per_commit_hash in
    match Hashtbl.find_opt tbl k with
    | Some (0, _) | None -> Fmt.failwith "Unexpected scope"
    | Some (remaining, instanciations_before) ->
        let scope_start =
          if instanciations_before = 0 then Def1.First_instanciation
          else Def1.Reinstanciation
        in
        let scope_end =
          if remaining = 1 then Def1.Last_occurence else Def1.Will_reoccur
        in
        if remaining = 1 then Hashtbl.remove tbl k
        else Hashtbl.replace tbl k (remaining - 1, instanciations_before + 1) ;
        (scope_start, scope_end, k)
  in
  let scopeh_lhs ids k =
    let tbl = ids.occurrence_count_per_commit_hash in
    match Hashtbl.find_opt tbl k with
    | Some (0, _) | None -> Fmt.failwith "Unexpected scope"
    | Some (remaining, instanciations_before) ->
        let scope_start =
          if instanciations_before = 0 then Def1.Not_instanciated
          else Def1.Instanciated
        in
        let scope_end =
          if remaining = 1 then Def1.Last_occurence else Def1.Will_reoccur
        in
        if remaining = 1 then Hashtbl.remove tbl k
        else Hashtbl.replace tbl k (remaining - 1, instanciations_before) ;
        (scope_start, scope_end, k)
  in
  let open Def0 in
  function
  | Def0.Tree v -> (
      let open Tree in
      match v with
      | Empty (x, t) ->
          {
            rank = `Use;
            tracker_ids = ([t], [x], []);
            to_v1 =
              (fun ids ->
                Def1.Tree.Empty (scopec ids x, scopet ids t) |> to_tree);
          }
      | Of_raw (x, t) ->
          {
            rank = `Use;
            tracker_ids = ([t], [], []);
            to_v1 = (fun ids -> Def1.Tree.Of_raw (x, scopet ids t) |> to_tree);
          }
      | Of_value ((x, v), t) ->
          {
            rank = `Use;
            tracker_ids = ([t], [x], []);
            to_v1 =
              (fun ids ->
                Def1.Tree.Of_value ((scopec ids x, v), scopet ids t) |> to_tree);
          }
      | Mem ((t, x), y) ->
          {
            rank = `Use;
            tracker_ids = ([t], [], []);
            to_v1 = (fun ids -> Def1.Tree.Mem ((scopet ids t, x), y) |> to_tree);
          }
      | Mem_tree ((t, x), y) ->
          {
            rank = `Use;
            tracker_ids = ([t], [], []);
            to_v1 =
              (fun ids -> Def1.Tree.Mem_tree ((scopet ids t, x), y) |> to_tree);
          }
      | Find ((t, x), y) ->
          {
            rank = `Use;
            tracker_ids = ([t], [], []);
            to_v1 =
              (fun ids ->
                Def1.Tree.Find ((scopet ids t, x), Option.is_some y) |> to_tree);
          }
      | Is_empty (t, x) ->
          {
            rank = `Use;
            tracker_ids = ([t], [], []);
            to_v1 = (fun ids -> Def1.Tree.Is_empty (scopet ids t, x) |> to_tree);
          }
      | Kind (t, x) ->
          {
            rank = `Use;
            tracker_ids = ([t], [], []);
            to_v1 = (fun ids -> Def1.Tree.Kind (scopet ids t, x) |> to_tree);
          }
      | Hash (t, _) ->
          {
            rank = `Use;
            tracker_ids = ([t], [], []);
            to_v1 = (fun ids -> Def1.Tree.Hash (scopet ids t, ()) |> to_tree);
          }
      | Equal ((t, t'), x) ->
          {
            rank = `Use;
            tracker_ids = ([t; t'], [], []);
            to_v1 =
              (fun ids ->
                Def1.Tree.Equal ((scopet ids t, scopet ids t'), x) |> to_tree);
          }
      | To_value (t, x) ->
          {
            rank = `Use;
            tracker_ids = ([t], [], []);
            to_v1 =
              (fun ids ->
                Def1.Tree.To_value (scopet ids t, Option.is_some x) |> to_tree);
          }
      | Clear ((x, t), ()) ->
          {
            rank = `Use;
            tracker_ids = ([t], [], []);
            to_v1 =
              (fun ids -> Def1.Tree.Clear ((x, scopet ids t), ()) |> to_tree);
          }
      | Find_tree ((t, x), t') ->
          {
            rank = `Use;
            tracker_ids = ([t] @ Option.to_list t', [], []);
            to_v1 =
              (fun ids ->
                Def1.Tree.Find_tree
                  ((scopet ids t, x), Option.map (scopet ids) t')
                |> to_tree);
          }
      | Add ((t, x, y), t') ->
          {
            rank = `Use;
            tracker_ids = ([t; t'], [], []);
            to_v1 =
              (fun ids ->
                Def1.Tree.Add ((scopet ids t, x, y), scopet ids t') |> to_tree);
          }
      | Add_tree ((t, x, t'), t'') ->
          {
            rank = `Use;
            tracker_ids = ([t; t'; t''], [], []);
            to_v1 =
              (fun ids ->
                Def1.Tree.Add_tree
                  ((scopet ids t, x, scopet ids t'), scopet ids t'')
                |> to_tree);
          }
      | Remove ((t, x), t') ->
          {
            rank = `Use;
            tracker_ids = ([t; t'], [], []);
            to_v1 =
              (fun ids ->
                Def1.Tree.Remove ((scopet ids t, x), scopet ids t') |> to_tree);
          }
      | List _ -> (* see List for rationale *) crash
      | Fold_start _ -> (* see List for rationale *) crash
      | Fold_step_enter _ -> crash
      | Fold_step_exit _ -> crash
      | Fold_end _ -> crash)
  | Find_tree ((c, x), t') ->
      {
        rank = `Use;
        tracker_ids = (Option.to_list t', [c], []);
        to_v1 =
          (fun ids ->
            Def1.Find_tree ((scopec ids c, x), Option.map (scopet ids) t'));
      }
  | Fold_start (x, c, y) ->
      {
        rank = `Use;
        tracker_ids = ([], [c], []);
        to_v1 = (fun ids -> Def1.Fold_start (x, scopec ids c, y));
      }
  | Fold_step_enter t ->
      {
        rank = `Use;
        tracker_ids = ([t], [], []);
        to_v1 = (fun ids -> Def1.Fold_step_enter (scopet ids t));
      }
  | Fold_step_exit t ->
      {
        rank = `Use;
        tracker_ids = ([t], [], []);
        to_v1 = (fun ids -> Def1.Fold_step_exit (scopet ids t));
      }
  | Fold_end _ ->
      {
        rank = `Use;
        tracker_ids = ([], [], []);
        to_v1 = (fun _ -> Def1.Fold_end);
      }
  | Add_tree ((c, x, t), c') ->
      {
        rank = `Use;
        tracker_ids = ([t], [c; c'], []);
        to_v1 =
          (fun ids ->
            Def1.Add_tree ((scopec ids c, x, scopet ids t), scopec ids c'));
      }
  | Mem ((c, x), y) ->
      {
        rank = `Use;
        tracker_ids = ([], [c], []);
        to_v1 = (fun ids -> Def1.Mem ((scopec ids c, x), y));
      }
  | Mem_tree ((c, x), y) ->
      {
        rank = `Use;
        tracker_ids = ([], [c], []);
        to_v1 = (fun ids -> Def1.Mem_tree ((scopec ids c, x), y));
      }
  | Find ((c, x), y) ->
      {
        rank = `Use;
        tracker_ids = ([], [c], []);
        to_v1 = (fun ids -> Def1.Find ((scopec ids c, x), Option.is_some y));
      }
  | Get_protocol (c, _) ->
      {
        rank = `Use;
        tracker_ids = ([], [c], []);
        to_v1 = (fun ids -> Def1.Get_protocol (scopec ids c, ()));
      }
  | Hash ((x, y, c), _) ->
      {
        rank = `Use;
        tracker_ids = ([], [c], []);
        to_v1 = (fun ids -> Def1.Hash ((x, y, scopec ids c), ()));
      }
  | Find_predecessor_block_metadata_hash (c, _) ->
      {
        rank = `Use;
        tracker_ids = ([], [c], []);
        to_v1 =
          (fun ids ->
            Def1.Find_predecessor_block_metadata_hash (scopec ids c, ()));
      }
  | Find_predecessor_ops_metadata_hash (c, _) ->
      {
        rank = `Use;
        tracker_ids = ([], [c], []);
        to_v1 =
          (fun ids ->
            Def1.Find_predecessor_ops_metadata_hash (scopec ids c, ()));
      }
  | Get_test_chain (c, _) ->
      {
        rank = `Use;
        tracker_ids = ([], [c], []);
        to_v1 = (fun ids -> Def1.Get_test_chain (scopec ids c, ()));
      }
  | Exists ((h, x), _) ->
      {
        rank = `Use;
        tracker_ids = ([], [], [h]);
        to_v1 = (fun ids -> Def1.Exists (scopeh_lhs ids h, x));
      }
  | Retrieve_commit_info ((_, _), _) -> ignore_
  | Add ((c, x, y), c') ->
      {
        rank = `Use;
        tracker_ids = ([], [c; c'], []);
        to_v1 = (fun ids -> Def1.Add ((scopec ids c, x, y), scopec ids c'));
      }
  | Remove ((c, x), c') ->
      {
        rank = `Use;
        tracker_ids = ([], [c; c'], []);
        to_v1 = (fun ids -> Def1.Remove ((scopec ids c, x), scopec ids c'));
      }
  | Add_protocol ((c, x), c') ->
      {
        rank = `Use;
        tracker_ids = ([], [c; c'], []);
        to_v1 =
          (fun ids -> Def1.Add_protocol ((scopec ids c, x), scopec ids c'));
      }
  | Add_predecessor_block_metadata_hash ((c, x), c') ->
      {
        rank = `Use;
        tracker_ids = ([], [c; c'], []);
        to_v1 =
          (fun ids ->
            Def1.Add_predecessor_block_metadata_hash
              ((scopec ids c, x), scopec ids c'));
      }
  | Add_predecessor_ops_metadata_hash ((c, x), c') ->
      {
        rank = `Use;
        tracker_ids = ([], [c; c'], []);
        to_v1 =
          (fun ids ->
            Def1.Add_predecessor_ops_metadata_hash
              ((scopec ids c, x), scopec ids c'));
      }
  | Add_test_chain ((c, x), c') ->
      {
        rank = `Use;
        tracker_ids = ([], [c; c'], []);
        to_v1 =
          (fun ids -> Def1.Add_test_chain ((scopec ids c, x), scopec ids c'));
      }
  | Remove_test_chain (c, c') ->
      {
        rank = `Use;
        tracker_ids = ([], [c; c'], []);
        to_v1 =
          (fun ids -> Def1.Remove_test_chain (scopec ids c, scopec ids c'));
      }
  | Fork_test_chain ((c, x, y), c') ->
      {
        rank = `Use;
        tracker_ids = ([], [c; c'], []);
        to_v1 =
          (fun ids ->
            Def1.Fork_test_chain ((scopec ids c, x, y), scopec ids c'));
      }
  | Checkout ((h, Some c), _) | Checkout_exn ((h, Ok c), _) ->
      {
        rank = `Control_flow;
        tracker_ids = ([], [c], [h]);
        to_v1 = (fun ids -> Def1.Checkout (scopeh_lhs ids h, scopec ids c));
      }
  | Commit_genesis_start (((chain_id, time_protocol, hash), ()), _) ->
      {
        rank = `Use;
        tracker_ids = ([], [], []);
        to_v1 =
          (fun _ ->
            Def1.Commit_genesis_start ((chain_id, time_protocol, hash), ()));
      }
  | Commit_genesis_end (((), Ok h), _) ->
      {
        rank = `Control_flow;
        tracker_ids = ([], [], [h]);
        to_v1 = (fun ids -> Def1.Commit_genesis_end ((), scopeh_rhs ids h));
      }
  | Clear_test_chain (x, ()) ->
      {
        rank = `Use;
        tracker_ids = ([], [], []);
        to_v1 = (fun _ -> Def1.Clear_test_chain (x, ()));
      }
  | Commit (((x, y, c), h), _) ->
      {
        rank = `Control_flow;
        tracker_ids = ([], [c], [h]);
        to_v1 =
          (fun ids -> Def1.Commit ((x, y, scopec ids c), scopeh_rhs ids h));
      }
  | Init (ro, ()) ->
      {
        rank = `Control_flow;
        tracker_ids = ([], [], []);
        to_v1 = (fun _ -> Def1.Init (ro, ()));
      }
  | Patch_context_enter c ->
      {
        rank = `Use;
        tracker_ids = ([], [c], []);
        to_v1 = (fun ids -> Def1.Patch_context_enter (scopec ids c));
      }
  | Patch_context_exit (c, Ok c') ->
      {
        rank = `Use;
        tracker_ids = ([], [c; c'], []);
        to_v1 =
          (fun ids -> Def1.Patch_context_exit (scopec ids c, scopec ids c'));
      }
  | List _ ->
      (* Since there are no occurences of it in the Raw trace, and since this is
         tricky to implement, let's not implement it yet to avoid introducing
         non-covered bugs *)
      crash
  | Merkle_tree _ ->
      (* No occurences in raw and missing types from opam's tezos libs *)
      ignore_
  | Commit_test_chain_genesis _ ->
      (* No occurences in raw but would requires a special handling in the stats *)
      crash
  | Checkout ((_, None), _)
  | Checkout_exn ((_, Error _), _)
  | Commit_genesis_end ((_, Error _), _)
  | Patch_context_exit (_, Error _) ->
      (* Let's not handle these failed calls *)
      crash
  | Close | Sync _ | Set_master _ | Set_head _ | Dump_context _ ->
      (* Let's not handle these calls. The replay will finish by closing the
         repo. *)
      crash
  | Unhandled Tree_shallow
  | Unhandled Tree_to_raw
  | Unhandled Tree_pp
  | Unhandled Tree_length
  | Unhandled Tree_stats
  | Unhandled Length
  | Unhandled Stats
  | Unhandled Restore_context
  | Unhandled Check_protocol_commit_consistency
  | Unhandled Validate_context_hash_consistency_and_commit
  | Unhandled Legacy_restore_context
  | Unhandled Legacy_restore_contexts
  | Unhandled Legacy_get_protocol_data_from_header
  | Unhandled Legacy_dump_snapshot
  | Unhandled
      ( Produce_tree_proof | Verify_tree_proof | Produce_stream_proof
      | Verify_stream_proof )
  | Unhandled
      ( Tree_config | Tree_kinded_key | Index_empty | Context_is_empty | Config
      | Equal_config | To_memory_tree ) ->
      crash

(* TODO: ignore everything or crash on some cases? *)

module Pass0 = struct
  (** Count events *)
  module Op_count = struct
    type t = {ignored : int; used : int}

    let folder =
      let acc0 = {ignored = 0; used = 0} in
      let accumulate acc ((_ : int64), row) =
        match event_infos row with
        | {rank = `Ignore; _} -> {acc with ignored = acc.ignored + 1}
        | _ -> {acc with used = acc.used + 1}
      in
      let finalise = Fun.id in
      Trace_common.Parallel_folders.folder acc0 accumulate finalise
  end

  (** List all tracker ids *)
  module Id_counts = struct
    type t = tracker_id_tables

    let folder =
      let acc0 =
        {
          occurrence_count_per_tree_id = Hashtbl.create 100_000_000;
          occurrence_count_per_context_id = Hashtbl.create 100_000_000;
          occurrence_count_per_commit_hash = Hashtbl.create 1_000_000;
        }
      in
      let accumulate acc ((i : int64), row) =
        if Int64.rem i 25_000_000L = 0L then
          Logs.app (fun l -> l "Pass0 - Dealing with row idx %#Ld" i) ;
        match event_infos row with
        | {rank = `Use; tracker_ids; _} | {rank = `Control_flow; tracker_ids; _}
          ->
            let (tree_ids, context_ids, commit_hashes) = tracker_ids in
            let incr tbl k =
              match Hashtbl.find_opt tbl k with
              | None -> Hashtbl.add tbl k 1
              | Some i -> Hashtbl.replace tbl k (i + 1)
            in
            List.iter (incr acc.occurrence_count_per_tree_id) tree_ids ;
            List.iter (incr acc.occurrence_count_per_context_id) context_ids ;
            let incr tbl k =
              match Hashtbl.find_opt tbl k with
              | None -> Hashtbl.add tbl k (1, 0)
              | Some (i, _) -> Hashtbl.replace tbl k (i + 1, 0)
            in
            List.iter (incr acc.occurrence_count_per_commit_hash) commit_hashes ;
            acc
        | _ -> acc
      in
      let finalise = Fun.id in
      Trace_common.Parallel_folders.folder acc0 accumulate finalise
  end

  (** Group the rows per block. *)
  module Segmentation = struct
    type block_summary = {
      first_row_idx : int64;
      last_row_idx : int64;
      checkout_hash : hash option;
      uses_patch_context : bool;
      commit_hash : hash;
      level : int option;
    }

    type acc = {
      events_since_commit : int;
      ingest_row : acc -> int64 * Def0.row -> acc;
      summary_per_block_rev : block_summary list;
      should_init : [`Absent | `Found of bool];
    }

    type t = block_summary list

    let rec look_for_block_start_of_raw acc (idx, row) =
      let open Def0 in
      let events_since_commit = acc.events_since_commit + 1 in
      match row with
      | Init (ro, _) ->
          if acc.should_init = `Absent then
            {
              acc with
              ingest_row = look_after_init idx;
              events_since_commit;
              should_init = `Found ro;
            }
          else
            Fmt.failwith
              "Two inits were found in the trace. It should not be possible."
      | row ->
          Fmt.failwith
            "Unexpected op at the beginning of a block for the row start: %a"
            (Repr.pp Def0.row_t)
            row

    and look_after_init first_row_idx acc (_, row) =
      let open Def0 in
      let events_since_commit = acc.events_since_commit + 1 in
      match (event_infos row, row) with
      | (_, (Checkout ((h, Some _), _) | Checkout_exn ((h, Ok _), _))) ->
          {
            acc with
            ingest_row = look_for_commit (Some first_row_idx) (Some h);
            events_since_commit;
          }
      | ({rank = `Use; _}, _) ->
          {
            acc with
            ingest_row = look_for_commit_genesis first_row_idx false;
            events_since_commit;
          }
      | ({rank = `Control_flow; _}, _)
      | ({rank = `Ignore; _}, _)
      | ({rank = `Crash; _}, _) ->
          Fmt.failwith
            "Can't handle the following raw event after an init: %a"
            (Repr.pp Def0.row_t)
            row

    and look_for_commit_genesis first_row_idx uses_patch_context acc (idx, row)
        =
      let open Def0 in
      let events_since_commit = acc.events_since_commit + 1 in
      match (event_infos row, row) with
      | ({rank = `Ignore; _}, _) -> acc
      | (_, Patch_context_enter _) ->
          assert (not uses_patch_context) ;
          {
            acc with
            ingest_row = look_for_commit_genesis first_row_idx true;
            events_since_commit;
          }
      | ({rank = `Use; _}, _) -> {acc with events_since_commit}
      | ({rank = `Control_flow; _}, Commit_genesis_end ((_, Ok h), _)) ->
          let block =
            {
              first_row_idx;
              last_row_idx = idx;
              level = Some 0;
              checkout_hash = None;
              commit_hash = h;
              uses_patch_context;
            }
          in
          {
            events_since_commit = 0;
            ingest_row = look_for_commit None None;
            summary_per_block_rev = block :: acc.summary_per_block_rev;
            should_init = acc.should_init;
          }
      | ({rank = `Crash; _}, _) | ({rank = `Control_flow; _}, _) ->
          Fmt.failwith
            "Can't handle the following raw event: %a"
            (Repr.pp Def0.row_t)
            row
            Fmt.failwith
            "Unexpected op at the end of a block: %a"
            (Repr.pp Def0.row_t)
            row

    and look_for_commit first_row_idx checkout_hash acc (idx, row) =
      let open Def0 in
      let events_since_commit = acc.events_since_commit + 1 in
      match (event_infos row, row) with
      | ({rank = `Ignore; _}, _) -> acc
      | (_, (Checkout ((h, Some _), _) | Checkout_exn ((h, Ok _), _))) ->
          {
            acc with
            ingest_row = look_for_commit (Some idx) (Some h);
            events_since_commit;
          }
      | (_, Get_protocol (_, h)) ->
          {
            acc with
            ingest_row = look_for_commit (Some idx) (Some h);
            events_since_commit;
          }
      | ({rank = `Use; _}, _) ->
          {acc with events_since_commit; ingest_row = look_for_commit None None}
      | (_, Commit (((_, m, _), h), _)) ->
          let lvl = Option.map level_of_commit_message m in
          let first_row_idx =
            match first_row_idx with
            | None ->
                Fmt.failwith
                  "Unexpected empty first_row_idx during a commit: %a"
                  (Repr.pp Def0.row_t)
                  row
            | Some idx -> idx
          in
          let checkout_hash =
            match checkout_hash with
            | None ->
                Fmt.failwith
                  "Unexpected empty checkout hash during a commit: %a"
                  (Repr.pp Def0.row_t)
                  row
            | checkout_hash -> checkout_hash
          in
          let block =
            {
              first_row_idx;
              last_row_idx = idx;
              level = lvl;
              checkout_hash;
              commit_hash = h;
              uses_patch_context = false;
            }
          in
          {
            events_since_commit = 0;
            ingest_row = look_for_commit None None;
            summary_per_block_rev = block :: acc.summary_per_block_rev;
            should_init = acc.should_init;
          }
      | ({rank = `Control_flow; _}, _)
      | (_, Patch_context_enter _)
      | ({rank = `Crash; _}, _) ->
          Fmt.failwith
            "Can't handle the following raw event: %a"
            (Repr.pp Def0.row_t)
            row

    let folder =
      let acc0 =
        {
          ingest_row = look_for_block_start_of_raw;
          summary_per_block_rev = [];
          events_since_commit = 0;
          should_init = `Absent;
        }
      in
      let accumulate acc row = acc.ingest_row acc row in
      let finalise {summary_per_block_rev; should_init; _} =
        (List.rev summary_per_block_rev, should_init)
      in
      Trace_common.Parallel_folders.folder acc0 accumulate finalise
  end
end

(** Guess missing block levels *)
module Pass1 = struct
  type block_summary = {
    first_row_idx : int64;
    last_row_idx : int64;
    checkout_hash : hash option;
    commit_hash : hash;
    uses_patch_context : bool;
    level : int;
  }

  let check_result l =
    List.iter
      (fun i -> if i < 0 then Fmt.failwith "Illegal negative block level")
      l ;
    iter_2by2
      (fun a b ->
        if a + 1 = b then ()
        else if a = b then ()
        else Logs.app (fun l -> l "Pass1 - From block %#d to %#d\n%!" a b))
      l

  let interpolate_block_ids pass0_seg =
    let l =
      let open Pass0.Segmentation in
      List.rev_map
        (function
          | {level = Some level; _} -> `Present level
          | {level = None; _} -> `Missing)
        pass0_seg
      |> List.rev
    in
    let l =
      let map_middle prev_opt v next_opt =
        match (prev_opt, v, next_opt) with
        | (_, `Present v, _) -> v
        | (None, `Missing, None) ->
            Fmt.failwith
              "Can't interpolate block level when only 1 missing block"
        | (None, `Missing, Some (`Missing | `Present _)) ->
            Fmt.failwith "Can't interpolate block level when first is absent"
        | (Some (`Missing | `Present _), `Missing, None) ->
            Fmt.failwith "Can't interpolate block level when last is absent"
        | (Some `Missing, `Missing, _) | (_, `Missing, Some `Missing) ->
            Fmt.failwith "Can't interpolate block level when 2 in a row absent"
        | (Some (`Present prev), `Missing, Some (`Present next)) ->
            if prev + 2 = next then prev + 1
            else if prev + 1 = next then
              Fmt.failwith "Can't interpolate orphan block level"
            else Fmt.failwith "Unexpected block levels"
      in
      convolve_with_padding map_middle l
    in
    check_result l ;
    List.rev_map2
      (fun Pass0.Segmentation.
             {
               first_row_idx;
               last_row_idx;
               checkout_hash;
               commit_hash;
               uses_patch_context;
               _;
             }
           level ->
        {
          level;
          first_row_idx;
          last_row_idx;
          checkout_hash;
          commit_hash;
          uses_patch_context;
        })
      pass0_seg
      l
    |> List.rev
end

(** Write to destination channel *)
module Pass2 = struct
  let check_genesis_is_last_block ops =
    let length = Array.length ops in
    if length <= 0 then
      Fmt.failwith
        "The last operation of the first block is supposed to be a commit \
         genesis but the array as a length of 0."
    else
      match ops.(length - 1) with
      | Def1.Commit_genesis_end _ -> ()
      | _ ->
          Fmt.failwith
            "The last operation of the first block is supposed to be a commit \
             genesis it's not the case."

  let rec run ?(depth = 0) tzstats_cache ids row_seq writer block_summaries
      read_only should_init =
    match block_summaries with
    | [] -> Lwt.return_unit
    | Pass1.{last_row_idx; level; uses_patch_context; _} :: tl ->
        if level mod 20_000 = 0 then
          Logs.app (fun l -> l "Pass2 - Dealing with block lvl %#d" level) ;
        let (row_seq, rows) =
          Seq.Custom.take_up_to
            ~is_last:(fun (i, _) -> i = last_row_idx)
            row_seq
        in
        let ops =
          rows
          |> List.filter_map (fun (_, row) ->
                 match event_infos row with
                 | {rank = `Ignore; _} -> None
                 | {to_v1; _} -> Some (to_v1 ids))
        in
        let ops =
          if depth = 0 && should_init = `Absent then
            (* Inject an [Init] op in case we sliced the input sequence to start
               from elsewhere than the beginning of the raw trace. *)
            Def1.Init (read_only, ()) :: ops |> Array.of_list
          else Array.of_list ops
        in
        if level = 0 then check_genesis_is_last_block ops ;
        let* Tzstat_metrics.
               {
                 op_count;
                 op_count_tx;
                 op_count_contract;
                 gas_used;
                 storage_size;
                 is_cycle_snapshot;
                 time;
                 solvetime;
                 _;
               } =
          (* The current commit might actually be an orphan block. The number
             of operations will be wrong in that case. *)
          Tzstat_metrics.fetch tzstats_cache level
        in
        let row =
          Def1.
            {
              level;
              ops;
              tzop_count = op_count;
              tzop_count_tx = op_count_tx;
              tzop_count_contract = op_count_contract;
              tz_gas_used = gas_used;
              tz_storage_size = storage_size;
              tz_cycle_snapshot = is_cycle_snapshot;
              tz_time = time;
              tz_solvetime = solvetime;
              uses_patch_context;
            }
        in
        Def1.append_row writer row ;
        run
          ~depth:(depth + 1)
          tzstats_cache
          ids
          row_seq
          writer
          tl
          read_only
          should_init
end

let run ?(first = `Idx 0) ?(last = `End) in_path out_chan =
  (match Sys.getenv_opt "CONDUIT_TLS" with
  | Some "openssl" -> ()
  | _ -> Logs.warn (fun l -> l "CONDUIT_TLS is not openssl")) ;

  (* Choose the right file in the raw trace directory *)
  let in_paths = Def0.trace_files_of_trace_directory ~filter:[`Rw] in_path in
  if List.length in_paths <> 1 then
    Fmt.failwith
      "Expecting exactly one RW trace file in %s. Got %d"
      in_path
      (List.length in_paths) ;
  let (in_path, _pid) = List.hd in_paths in
  Logs.app (fun l -> l "Using %s" in_path) ;

  (* Pass 0, segment blocks, summarise tracker ids *)
  let (op_counts, ids, (seg, should_init)) =
    let construct op_counts ids seg = (op_counts, ids, seg) in
    let pf0 =
      let open Trace_common.Parallel_folders in
      open_ construct |+ Pass0.Op_count.folder |+ Pass0.Id_counts.folder
      |+ Pass0.Segmentation.folder |> seal
    in
    Def0.open_reader in_path
    |> (fun (_, _, x) -> x)
    |> Slice_input_seq.slice ~first ~last
    |> Seq.Custom.mapi64
    |> Seq.fold_left Trace_common.Parallel_folders.accumulate pf0
    |> Trace_common.Parallel_folders.finalise
  in
  let () =
    let count = List.length seg in
    Logs.app (fun l ->
        l
          "Pass0 done. \n\
          \  Found %#d blocks, %#d trees, %#d contexts, %#d commit hashes. \n\
          \  %#d operations ignored and %#d operations used."
          count
          (Hashtbl.length ids.occurrence_count_per_tree_id)
          (Hashtbl.length ids.occurrence_count_per_context_id)
          (Hashtbl.length ids.occurrence_count_per_commit_hash)
          op_counts.Pass0.Op_count.ignored
          op_counts.Pass0.Op_count.used)
  in

  (* Pass 1, fill the holes in segmentation *)
  Logs.app (fun l -> l "Pass1") ;
  let seg = Pass1.interpolate_block_ids seg in
  let () =
    let count = List.length seg in
    Logs.app (fun l ->
        l
          "Pass1 done. \n  First/last block levels are %#d/%#d"
          (if count = 0 then 0 else (List.hd seg).level)
          (if count = 0 then 0 else (List.nth seg (count - 1)).level))
  in

  (* Check if we have an Init block and if the value is read only. *)
  let read_only =
    match Def0.type_of_file in_path with
    | `Ro -> true
    | `Rw -> false
    | `Misc ->
        Fmt.failwith
          "Init block not found in the first blocks. It should not happend."
  in

  (match should_init with
  | `Absent -> ()
  | `Found true ->
      if not read_only then
        Fmt.failwith
          "Found an Init block of type Rw and another of type Ro. Types \
           mismatch."
  | `Found false ->
      if read_only then
        Fmt.failwith
          "Found an init of type Ro and another of type Rw. Types mismatch.") ;

  (* Pass 2, write to destination channel *)
  Logs.app (fun l -> l "Pass2") ;
  let reader =
    Def0.open_reader in_path
    |> (fun (_, _, x) -> x)
    |> Slice_input_seq.slice ~first ~last
    |> Seq.Custom.mapi64
  in
  let header =
    let block_count = List.length seg in
    let initial_block =
      let record = List.hd seg in
      match record.Pass1.checkout_hash with
      | None -> None
      | Some h -> Some (record.Pass1.level - 1, h)
    in
    let last_block =
      let record = List.nth seg (block_count - 1) in
      Pass1.(record.level, record.commit_hash)
    in
    Def1.{block_count; initial_block; last_block}
  in
  let writer = Def1.create out_chan header in
  let tzstats_cache =
    Tzstat_metrics.create
      Filename.(concat (get_temp_dir_name ()) "tzstats-cache")
  in
  Pass2.run tzstats_cache ids reader writer seg read_only should_init
