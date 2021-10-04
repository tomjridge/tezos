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

module Seq = struct
  include Seq

  (* Backported from OCaml 4.11 *)
  let rec unfold f u () =
    match f u with None -> Nil | Some (x, u') -> Cons (x, unfold f u')

  module Custom = struct
    let take count_expected seq =
      let rec aux seq took_rev count_sofar =
        if count_sofar = count_expected then List.rev took_rev
        else
          match seq () with
          | Seq.Nil -> List.rev took_rev
          | Cons (v, seq) -> aux seq (v :: took_rev) (count_sofar + 1)
      in
      aux seq [] 0

    let mapi64 s =
      let i = ref (-1L) in
      Seq.map
        (fun v ->
          i := Int64.succ !i ;
          (!i, v))
        s

    let take_up_to ~is_last seq =
      let rec aux seq rev_l =
        match seq () with
        | Nil ->
            (* Reached the end without [is_last] *)
            (seq, rev_l)
        | Cons (v, seq) ->
            let rev_l = v :: rev_l in
            if is_last v then (seq, rev_l) else aux seq rev_l
      in
      let (seq, rev_l) = aux seq [] in
      (seq, List.rev rev_l)

    let take_until ~is_last seq =
      let aux (stop, seq) =
        if stop then None
        else
          match seq () with
          | Seq.Nil -> None
          | Seq.Cons (v, rest) ->
              if is_last v then Some (v, (true, rest))
              else Some (v, (stop, rest))
      in
      Seq.unfold aux (false, seq)
  end
end

module Parallel_folders = struct
  type ('row, 'acc, 'v) folder = {
    acc : 'acc;
    accumulate : 'acc -> 'row -> 'acc;
    finalise : 'acc -> 'v;
  }

  let folder acc accumulate finalise = {acc; accumulate; finalise}

  type ('res, 'row, 'v) folders =
    | F0 : ('res, 'row, 'res) folders
    | F1 :
        ('row, 'acc, 'v) folder * ('res, 'row, 'rest) folders
        -> ('res, 'row, 'v -> 'rest) folders

  type ('res, 'row, 'f, 'rest) open_t =
    ('res, 'row, 'rest) folders -> 'f * ('res, 'row, 'f) folders

  let open_ : 'f -> ('res, 'row, 'f, 'f) open_t =
   fun constructor folders -> (constructor, folders)

  let app :
      type res f v rest acc row.
      (res, row, f, v -> rest) open_t ->
      (row, acc, v) folder ->
      (res, row, f, rest) open_t =
   fun open_t folder folders -> open_t (F1 (folder, folders))

  let ( |+ ) = app

  type ('res, 'row) t = T : 'f * ('res, 'row, 'f) folders -> ('res, 'row) t

  let seal : type res row f. (res, row, f, res) open_t -> (res, row) t =
   fun open_t ->
    let (constructor, folders) = open_t F0 in
    T (constructor, folders)

  let accumulate : type res row. (res, row) t -> row -> (res, row) t =
   fun (T (constructor, folders)) row ->
    let rec aux : type v. (res, row, v) folders -> (res, row, v) folders =
      function
      | F0 -> F0
      | F1 (folder, t) as f -> (
          let acc = folder.acc in
          let acc' = folder.accumulate acc row in
          let t' = aux t in
          (* Avoid reallocating [F1] and [folder] when possible. *)
          match (acc == acc', t == t') with
          | (true, true) -> f
          | (true, false) -> F1 (folder, t')
          | (false, (true | false)) -> F1 ({folder with acc = acc'}, t'))
    in
    let folders = aux folders in
    T (constructor, folders)

  let finalise : type res row. (res, row) t -> res =
    let rec aux : type c. (res, row, c) folders -> c -> res = function
      | F0 -> Fun.id
      | F1 (f, fs) ->
          fun constructor ->
            let v = f.finalise f.acc in
            let finalise_remaining = aux fs in
            let constructor = constructor v in
            finalise_remaining constructor
    in
    fun (T (constructor, folders)) -> aux folders constructor
end
