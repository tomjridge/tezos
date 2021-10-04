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

(* Using [exception] instead of [error] because all occurences are in direct
   code *)
exception Recording_io_error of string

exception Suspicious_trace_file of string

exception Stats_trace_without_init

exception Raw_trace_without_init

let prepare_trace_file prefix filename =
  let rec mkdir_p path =
    let exists = Sys.file_exists path in
    let is_dir = Sys.is_directory path in
    if exists && is_dir then ()
    else if exists then
      let msg =
        Fmt.str
          "Failed to prepare trace destination directory, '%s' is not a \
           directory"
          path
      in
      raise (Recording_io_error msg)
    else
      let path' = Filename.dirname path in
      let () =
        if path' = path then
          raise
            (Recording_io_error "Failed to prepare trace destination directory")
      in
      mkdir_p path' ;
      Unix.mkdir path 0o755
  in
  mkdir_p prefix ;
  let path = Filename.concat prefix filename in
  let () =
    if Sys.file_exists path then
      let msg = Fmt.str "Failed to create trace file '%s', file exists" path in
      raise (Recording_io_error msg)
  in
  path
