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

(** Stats trace summary configuration. *)

let histo_bin_count = 16

let curves_sample_count = 201

(** Parameter to control the width of the moving average window.

    This width is expressed as a fraction of the width of the final plot images,
    (i.e. the number of played blocks). This implies that the amount of
    smoothing remains visually the same, no matter [curves_sample_count] and the
    number of blocks in the stats trace.

    Justification of the current value:

    The 2nd commit of the full replay trace is a massive one, it contains ~1000x
    more operations than an average one, it takes ~10 half lives to fully get
    rid of it from the EMAs. With [moving_average_half_life_ratio = 1. /. 80.],
    that 2nd commit will only poluate [1 / 8] of the width of the smoothed
    curves. *)
let moving_average_half_life_ratio = 1. /. 80.

(** See [Exponential_moving_average] *)
let moving_average_relevance_threshold = 0.999
