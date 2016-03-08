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
  let len = String.length name in
  let rec doit off acc =
    let open String in
    let idx = try index_from name off delimiter with _ -> len in
    let fst = sub name off (idx - off) in
    let idx' = idx + 1 in
    if idx' <= len then
      doit idx' (fst :: acc)
    else
      fst :: acc
  in
  List.rev (doit 0 [])

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

let map_error err reqd_string = 
  match err with
  | Unix.EEXIST -> `Error (`File_already_exists reqd_string)
  | Unix.EISDIR -> `Error (`Is_a_directory reqd_string)
  | Unix.ENOENT -> `Error (`No_directory_entry ("", reqd_string))
  | Unix.ENOSPC -> `Error `No_space
  | Unix.ENOTDIR -> `Error (`Not_a_directory reqd_string)
  | Unix.ENOTEMPTY -> `Error (`Directory_not_empty reqd_string)
  | Unix.EUNKNOWNERR i -> `Error (`Unknown_error 
                                    (reqd_string ^ ": error " ^ string_of_int i))

let err_catcher name = function
  | Unix.Unix_error (ex, _, _) -> return (map_error ex name)
  | exn -> Lwt.fail exn

let read_impl base name off reqd_len =
  if reqd_len < 0 then return (`Error (`Unknown_error "can't read negative bytes"))
  else begin
    let fullname = resolve_filename base name in
    Lwt.catch (fun () ->
        Lwt_unix.openfile fullname [Lwt_unix.O_RDONLY] 0 >>= fun fd ->
        Lwt_unix.lseek fd off Lwt_unix.SEEK_SET >>= fun _seek -> (* very little we can do with _seek *)
        let st =
          Lwt_stream.from (fun () ->
              let buf = Cstruct.create 4096 in
              Lwt_cstruct.read fd buf >>= fun len ->
              match len with
              | 0 ->
                Lwt_unix.close fd
                >>= fun () -> return None
              | len ->
                return (Some (Cstruct.sub buf 0 (min len reqd_len)))
            )
        in
        Lwt_stream.to_list st >>= fun bufs ->
        match reqd_len with
        | 0 -> return (`Ok [])
        | n -> return (`Ok bufs) )
      (err_catcher name)
  end

let size_impl base name =
  let fullname = resolve_filename base name in
  Lwt.catch (fun () ->
    Lwt_unix.LargeFile.stat fullname >>= fun stat ->
    match stat.Lwt_unix.LargeFile.st_kind with
      | Lwt_unix.S_REG -> return (`Ok stat.Lwt_unix.LargeFile.st_size)
      | _ -> return (`Error (`Is_a_directory name)))
    (err_catcher name)
