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

open Cmdliner
module Rawdef = Tezos_context_recording.Raw_actions_trace_definition
module Summary = Tezos_context_recording.Trace_raw_actions_summary
module Trace_raw_actions_to_replayable =
  Tezos_context_replay.Raw_trace_actions_to_replayable

(* Get the common time. *)
let now_s () = Mtime.Span.to_s (Mtime_clock.elapsed ())

(* Init a log reporter. *)
let reporter ?(prefix = "") () =
  let report src level ~over k msgf =
    let k _ =
      over () ;
      k ()
    in
    let ppf = Fmt.stderr in
    let with_stamp h _tags k fmt =
      let dt = now_s () in
      Fmt.kpf
        k
        ppf
        ("%s%+04.3fs %a %a @[" ^^ fmt ^^ "@]@.")
        prefix
        dt
        Logs_fmt.pp_header
        (level, h)
        Fmt.(styled `Magenta string)
        (Logs.Src.name src)
    in
    msgf @@ fun ?header ?tags fmt -> with_stamp header tags k fmt
  in
  {Logs.report}

(* Initialize the log system. *)
let setup_log style_renderer level =
  Fmt_tty.setup_std_outputs ?style_renderer () ;
  Logs.set_level level ;
  Logs.set_reporter (reporter ()) ;
  ()

let summarise path =
  Summary.(summarise path |> Fmt.pr "%a\n" (Irmin.Type.pp_json t))

let to_replayable () path block_level_of_first_opt block_level_of_last_opt
    block_idx_of_first_opt block_count_opt =
  let first =
    match (block_idx_of_first_opt, block_level_of_first_opt) with
    | (None, None) -> `Idx 0
    | (Some _, Some _) ->
        invalid_arg
          "block-idx-of-first and block-level-of-first must not be used \
           together"
    | (Some i, None) -> `Idx i
    | (None, Some i) -> `Level i
  in
  let last =
    match (block_count_opt, block_level_of_last_opt) with
    | (None, None) -> `End
    | (Some _, Some _) ->
        invalid_arg
          "block-count and block-level-of-last must not be used together"
    | (Some i, None) -> `Count i
    | (None, Some i) -> `Level i
  in
  Lwt_main.run (Trace_raw_actions_to_replayable.run ~first ~last path stdout) ;
  flush stdout

let list path =
  Rawdef.trace_files_of_trace_directory path
  |> List.iter (fun (path, _) ->
         Fmt.pr "Reading %s\n" path ;
         let (_, (), row_seq) = Rawdef.open_reader path in
         Seq.iter (Fmt.pr "%a\n" (Repr.pp Rawdef.row_t)) row_seq) ;
  Fmt.pr "%!"

let classify path =
  match Rawdef.type_of_file path with
  | `Ro -> Fmt.pr "This file is of type RO.\n"
  | `Rw -> Fmt.pr "This file is of type RW.\n"
  | `Misc -> Fmt.pr "This file is of type Misc.\n"

let setup_log =
  Term.(const setup_log $ Fmt_cli.style_renderer () $ Logs_cli.level ())

let term_summarise =
  let stats_trace_file =
    let doc = Arg.info ~docv:"PATH" ~doc:"A raw actions trace directory" [] in
    Arg.(required @@ pos 0 (some string) None doc)
  in
  Term.(const summarise $ stats_trace_file)

let term_to_rep =
  let stats_trace_file =
    let doc = Arg.info ~docv:"PATH" ~doc:"A raw actions trace directory" [] in
    Arg.(required @@ pos 0 (some string) None doc)
  in

  let arg_block_level_of_first =
    let open Arg in
    let doc = Arg.info ~doc:"" ["block-level-of-first"] in
    let a = opt (some int) None doc in
    value a
  in
  let arg_block_level_of_last =
    let open Arg in
    let doc = Arg.info ~doc:"" ["block-level-of-last"] in
    let a = opt (some int) None doc in
    value a
  in
  let arg_block_idx_of_first =
    let open Arg in
    let doc = Arg.info ~doc:"" ["block-idx-of-first"] in
    let a = opt (some int) None doc in
    value a
  in
  let arg_block_count =
    let open Arg in
    let doc = Arg.info ~doc:"" ["block-count"] in
    let a = opt (some int) None doc in
    value a
  in

  Term.(
    const to_replayable $ setup_log $ stats_trace_file
    $ arg_block_level_of_first $ arg_block_level_of_last
    $ arg_block_idx_of_first $ arg_block_count)

let term_list =
  let stats_trace_file =
    let doc = Arg.info ~docv:"PATH" ~doc:"A raw actions trace directory" [] in
    Arg.(required @@ pos 0 (some string) None doc)
  in
  Term.(const list $ stats_trace_file)

let term_classify =
  let stats_trace_file =
    let doc = Arg.info ~docv:"FILE" ~doc:"A raw actions trace file" [] in
    Arg.(required @@ pos 0 (some string) None doc)
  in
  Term.(const classify $ stats_trace_file)

let () =
  let man = [] in
  let i = Term.info ~man ~doc:"Processing of actions traces." "actions" in

  let man =
    [
      `P "From raw actions trace (directory) to summary (json).";
      `S "EXAMPLE";
      `P
        "manage_actions.exe summarise ./raw_actions/ \
         >./raw_actions_summary.json";
    ]
  in
  let j = Term.info ~man ~doc:"Raw Actions Summary" "summarise" in

  let man =
    [
      `P "From raw actions trace (directory) to replayable actions trace (file).";
      `S "EXAMPLE";
      `P
        "manage_actions.exe to-replayable ./raw_actions/ \
         >./replayable_actions.trace ";
    ]
  in
  let k = Term.info ~man ~doc:"Replayable Actions Trace" "to-replayable" in

  let man = [`P "List the operations from a raw actions trace (directory)."] in
  let l = Term.info ~man ~doc:"List Raw Actions" "list" in

  let man = [`P "Expose the type (RO, RW or Misc) of a raw trace."] in
  let m = Term.info ~man ~doc:"Check raw trace type" "classify" in

  Term.exit
  @@ Term.eval_choice
       (term_summarise, i)
       [
         (term_summarise, j);
         (term_to_rep, k);
         (term_list, l);
         (term_classify, m);
       ]
