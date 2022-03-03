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
module Trace_replay = Tezos_context_replay.Trace_replay

type indexing_strategy = Always | Minimal | Contents

let main indexing_strategy block_count startup_store_type replayable_trace_path
    artefacts_dir keep_store keep_stats_trace no_summary empty_blobs
    stats_trace_message no_pp_summary =
  let startup_store_type =
    match startup_store_type with None -> `Fresh | Some v -> `Copy_from v
  in
  let indexing_strategy =
    match indexing_strategy with
    | Always -> `Always
    | Minimal -> `Minimal
    | Contents -> `Contents
  in
  let module Replay =
    Trace_replay.Make
      (Tezos_context.Context)
      (struct
        let v : Trace_replay.config =
          {
            block_count;
            startup_store_type;
            replayable_trace_path;
            artefacts_dir;
            keep_store;
            keep_stats_trace;
            no_summary;
            empty_blobs;
            stats_trace_message;
            no_pp_summary;
            indexing_strategy;
          }
      end)
  in
  Replay.run ()

let indexing_strategy =
  let doc = "Specify the indexing_strategy to run when doing the replay." in
  let strategy =
    Arg.enum [("always", Always); ("minimal", Minimal); ("contents", Contents)]
  in
  Arg.(
    required
    & opt (some strategy) (Some Minimal)
    & info ["s"; "indexing-strategy"] ~doc)

let block_count =
  let doc =
    Arg.info ~doc:"Maximum number of blocks to read from trace." ["block-count"]
  in
  Arg.(value @@ opt (some int) None doc)

let startup_store_type =
  let doc =
    Arg.info
      ~docv:"PATH"
      ~doc:
        "Path to a directory that contains and irmin-pack store (i.e Tezos \
         data context directory) that shall serve as a basis for replay. The \
         provided path is not modified. If not provided, the replay starts \
         from a fresh store and the input actions trace should begin from the \
         genesis block."
      ["startup-store-copy"]
  in
  Arg.(value @@ opt (some string) None doc)

let replayable_trace_path =
  let doc =
    Arg.info
      ~docv:"TRACE_PATH"
      ~doc:"Trace of Tezos context operations to be replayed."
      []
  in
  Arg.(required @@ pos 0 (some string) None doc)

let artefacts_dir =
  let doc =
    Arg.info
      ~docv:"ARTEFACTS_PATH"
      ~doc:"Destination of the bench artefacts."
      []
  in
  Arg.(required @@ pos 1 (some string) None doc)

let keep_store =
  let doc =
    Arg.info
      ~doc:
        "Whether or not the irmin store should be discarded at the end. If \
         kept, the store will remain in the [ARTEFACTS_PATH] directory."
      ["keep-store"]
  in
  Arg.(value @@ flag doc)

let keep_stats_trace =
  let doc =
    Arg.info
      ~doc:
        "Whether or not the stats trace should be discarded are the end, after \
         the summary has been saved the disk. If kept, the stats trace will \
         remain in the [ARTEFACTS_PATH] directory"
      ["keep-stats-trace"]
  in
  Arg.(value @@ flag doc)

let no_summary =
  let doc =
    Arg.info
      ~doc:
        "Whether or not the stats trace should be converted to a summary at \
         the end of a replay."
      ["no-summary"]
  in
  Arg.(value @@ flag doc)

let empty_blobs =
  let doc =
    Arg.info
      ~doc:
        "Whether or not the blobs added to the store should be the empty \
         string, during trace replay. This greatly increases the replay speed."
      ["empty-blobs"]
  in
  Arg.(value @@ flag doc)

let stats_trace_message =
  let doc =
    Arg.info
      ~docv:"MESSAGE"
      ~doc:
        "Raw text to be stored in the header of the stats trace. Typically, a \
         JSON-formatted string that describes the setup of the benchmark(s)."
      ["stats-trace-message"]
  in
  Arg.(value @@ opt (some string) None doc)

let no_pp_summary =
  let doc =
    Arg.info
      ~doc:
        "Whether or not the summary should be displayed at the end of a replay."
      ["no-pp-summary"]
  in
  Arg.(value @@ flag doc)

let main_t =
  Term.(
    const main $ indexing_strategy $ block_count $ startup_store_type
    $ replayable_trace_path $ artefacts_dir $ keep_store $ keep_stats_trace
    $ no_summary $ empty_blobs $ stats_trace_message $ no_pp_summary)

let () =
  let info =
    Term.info ~doc:"Replay operation from a raw actions trace." "replay"
  in
  Term.exit @@ Term.eval (main_t, info)
