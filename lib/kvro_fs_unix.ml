(*
 * Copyright (c) 2013 Anil Madhavapeddy <anil@recoil.org>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *)

open Lwt

type +'a io = 'a Lwt.t
type id = string
type error =
  | Unknown_key of string
type page_aligned_buffer = Cstruct.t

type t = {
  base: string
}

let connect id =
  (* TODO verify base directory exists *)
  return (`Ok { base=id })

let disconnect t =
  return ()

let id {base} = base

let read {base} name off len =
  prerr_endline ("read: " ^ name);
  let fullname = Filename.concat base name in
  try_lwt
    Lwt_unix.openfile fullname [Lwt_unix.O_RDONLY] 0
    >>= fun fd ->
    let st =
      Lwt_stream.from (fun () ->
        let buf = Cstruct.create 4096 in
        lwt len = Lwt_cstruct.read fd buf in
        match len with
        | 0 ->
           Lwt_unix.close fd
           >>= fun () -> return None
        | len ->
           return (Some (Cstruct.sub buf 0 len))
        )
    in
    Lwt_stream.to_list st
    >>= fun bufs ->
    return (`Ok bufs)
   with exn ->
     return (`Error (Unknown_key name))

let size {base} name =
  prerr_endline ("size: " ^ name);
  let fullname = Filename.concat base name in
  try_lwt
    Lwt_unix.stat fullname 
    >>= fun stat ->
    let size = Int64.of_int (stat.Lwt_unix.st_size) in
    return (`Ok size)
  with exn ->
    return (`Error (Unknown_key name))

