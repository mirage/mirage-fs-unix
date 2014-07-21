(*
 * Copyright (c) 2013 Anil Madhavapeddy <anil@recoil.org>
 * Copyright (c) 2014 Thomas Gazagnaire <thomas@gazagnaire.org>
 * Copyright (c) 2014 Hannes Mehnert <hannes@mehnert.org>
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

let split_string delimiter name =
  let rec doit part acc =
    let open String in
    let len = length part in
    let idx = try index part delimiter with _ -> len in
    let fst = sub part 0 idx in
    let idx' = idx + 1 in
    if idx' <= len then
      let rt = sub part idx' (len - idx') in
      doit rt (fst :: acc)
    else
      fst :: acc
  in
  List.rev (doit name [])

let rec remove_dots parts outp =
  match parts, outp with
  | ".."::r, a::rt -> remove_dots r  rt
  | ".."::r, []    -> remove_dots r  []
  | "."::r , rt    -> remove_dots r  rt
  | r::rs  , rt    -> remove_dots rs (r :: rt)
  | []     , rt    -> List.rev rt

let resolve_filename base filename =
  let parts = split_string '/' filename in
  let name = remove_dots parts [] |> String.concat "/" in
  Filename.concat base name

let read_impl base name off len =
  prerr_endline ("read: " ^ name);
  let fullname = resolve_filename base name in
  try_lwt
    Lwt_unix.openfile fullname [Lwt_unix.O_RDONLY] 0 >>= fun fd ->
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
    Lwt_stream.to_list st >>= fun bufs ->
    return (`Ok bufs)
  with exn ->
    return (`Error (`No_directory_entry (base, name)))

let size_impl base name =
  prerr_endline ("size: " ^ name);
  let fullname = resolve_filename base name in
  try_lwt
    Lwt_unix.LargeFile.stat fullname >>= fun stat ->
    let size = stat.Lwt_unix.LargeFile.st_size in
    return (`Ok size)
  with exn ->
    return (`Error (`No_directory_entry (base, name)))
