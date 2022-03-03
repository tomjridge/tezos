(*****************************************************************************)
(*                                                                           *)
(* Open Source License                                                       *)
(* Copyright (c) 2022-2022 Tarides <contact@tarides.com>                     *)
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

(** Context comes with two variants: [Context] and [Context_binary] with
    different tradeoffs.

    Both have different Merkle tree representations (i.e. when presented the
    same data, they don't produce the same hashes).

    [lib_context] represents directories as a structured tree of inodes, instead
    of a flat list of files, to get efficient copy-on-write and optimised read
    patterns.

    The context variants differ by the branching factors used for these inode
    trees:

    - [Context] uses a branching factor of 32;
    - [Context_binary] uses a branching factor of 2.

    To represent a large directory, [Context] uses less but larger inodes than
    [Context_binary].

    As persisting inodes on disk have an overhead (i.e. the serialisation of an
    inode is prefixed by its 32 byte hash), [Context] is thus optimised for
    storing a large quantity of data on disk.

    On the opposite, as the inodes in Merkle proofs contain the hashes of the
    shallow siblings, [Context_binary] is thus optimised for producing smaller
    Merkle proofs. *)

module Context_binary = struct
  type error +=
    | Cannot_create_file = Context.Cannot_create_file
    | Cannot_open_file = Context.Cannot_open_file
    | Cannot_find_protocol = Context.Cannot_find_protocol
    | Suspicious_file = Context.Suspicious_file

  include Context.Make (Tezos_context_encoding.Context_binary)
end

(** The context of a tezos node. Persisted to disk. *)
module Context = struct
  type error +=
    | Cannot_create_file = Context.Cannot_create_file
    | Cannot_open_file = Context.Cannot_open_file
    | Cannot_find_protocol = Context.Cannot_find_protocol
    | Suspicious_file = Context.Suspicious_file

  let make_wrapped_context () :
      (module Tezos_context_sigs.Context.MACHIN
         with type memory_tree = Tezos_context_memory.Context.tree) =
    let module M = struct
      module Impl :
        Tezos_context_sigs.Context.MACHIN
          with type memory_tree = Tezos_context_memory.Context.tree =
        Context.Make (Tezos_context_encoding.Context)

      module type RECORDER =
        Tezos_context_recording.Recorder.S with module Impl = Impl
    end in
    let raw_actions_recorder : (module M.RECORDER) option =
      match Env.(v.record_raw_actions_trace) with
      | `No -> None
      | `Yes prefix ->
          (* Create a raw actions trace file in the [prefix] directory for the
             current process. Multiple processes will record to their own files.
          *)
          Some
            (module Tezos_context_recording.Raw_actions_trace_recorder.Make
                      (M.Impl)
                      (struct
                        let prefix = prefix
                      end))
    in
    let stats_recorder : (module M.RECORDER) option =
      match Env.(v.record_stats_trace) with
      | `No -> None
      | `Yes prefix ->
          (* The stats trace file will be created during the first call to [init].
             Multiple processes will record to their own files. *)
          Some
            (module Tezos_context_recording.Stats_trace_recorder.Make
                      (M.Impl)
                      (struct
                        let prefix = prefix

                        let message = Env.(v.stats_trace_message)
                      end))
    in
    (module Tezos_context_recording.Shim.Make
              (M.Impl)
              (struct
                module type RECORDER = M.RECORDER

                let l =
                  Option.to_list raw_actions_recorder
                  @ Option.to_list stats_recorder
              end))

  include
    (val match Env.(v.record_raw_actions_trace, v.record_stats_trace) with
         | (`No, `No) ->
             (module Context.Make (Tezos_context_encoding.Context)
             : Tezos_context_sigs.Context.MACHIN
               with type memory_tree = Tezos_context_memory.Context.tree)
         | (`Yes _, _) | (_, `Yes _) ->
             (* Enable recording for one or both of the trace kind.
                Seen from outside of [Tezos_context] these recordings are
                seemless, except for:
                - logging of important informations,
                - exceptions on IO error,
                - the creation of files that will be closed [at_exit].
             *)
             make_wrapped_context ())
end

module Context_dump = Context_dump
