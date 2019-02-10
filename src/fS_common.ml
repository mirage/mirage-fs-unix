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

open Lwt.Infix

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
  | ".."::r, _::rt -> remove_dots r  rt
  | ".."::r, []    -> remove_dots r  []
  | "."::r , rt    -> remove_dots r  rt
  | r::rs  , rt    -> remove_dots rs (r :: rt)
  | []     , rt    -> List.rev rt

let resolve_filename base filename =
  let parts = split_string '/' filename in
  let name = remove_dots parts [] |> String.concat "/" in
  Filename.concat base name

type fs_error = [
  | `Unix_error of Unix.error
  | `Unix_errorno of int
  | `Negative_bytes
]

type error = [ Mirage_fs.error | fs_error ]
type write_error = [ Mirage_fs.write_error | fs_error | `Directory_not_empty ]

let pp_fs_error ppf = function
  | `Unix_errorno i -> Fmt.pf ppf "UNIX errorno: %d" i
  | `Unix_error e   -> Fmt.pf ppf "UNIX error: %s" @@ Unix.error_message e
  | `Negative_bytes -> Fmt.string ppf "can't read negative bytes"

let pp_error ppf = function
  | #Mirage_fs.error as e -> Mirage_fs.pp_error ppf e
  | #fs_error as e        -> pp_fs_error ppf e

let pp_write_error ppf = function
  | #Mirage_fs.write_error as e -> Mirage_fs.pp_write_error ppf e
  | #fs_error as e              -> pp_fs_error ppf e
  | `Directory_not_empty        -> Fmt.string ppf "XXX"

let map_error = function
  | Unix.EISDIR -> Error `Is_a_directory
  | Unix.ENOENT -> Error `No_directory_entry
  | Unix.EUNKNOWNERR i -> Error (`Unix_errorno i)
  | e -> Error (`Unix_error e)

let map_write_error: Unix.error -> ('a, write_error) result = function
  | Unix.EEXIST -> Error `File_already_exists
  | Unix.EISDIR -> Error `Is_a_directory
  | Unix.ENOENT -> Error `No_directory_entry
  | Unix.ENOSPC -> Error `No_space
  | Unix.ENOTEMPTY -> Error `Directory_not_empty
  | Unix.EUNKNOWNERR i -> Error (`Unix_errorno i)
  | e -> Error (`Unix_error e)

let err_catcher = function
  | Unix.Unix_error (ex, _, _) -> Lwt.return (map_error ex)
  | exn -> Lwt.fail exn

let write_err_catcher = function
  | Unix.Unix_error (ex, _, _) -> Lwt.return (map_write_error ex)
  | exn -> Lwt.fail exn

let mem_impl base name =
  let fullname = resolve_filename base name in
  Lwt.catch (fun () ->
          Lwt_unix.stat fullname >>= fun _ -> Lwt.return (Ok true)
        )
  (function
          | Unix.Unix_error (Unix.ENOENT, _, _) -> Lwt.return (Ok false)
          | e -> err_catcher e)

let read_impl base key =
  let name = Mirage_kv.Key.to_string key in
  let fullname = resolve_filename base name in
  Lwt.catch (fun () ->
      Lwt_unix.openfile fullname [Lwt_unix.O_RDONLY] 0 >>= fun fd ->
      Lwt.finalize (fun () ->
          Lwt_unix.LargeFile.fstat fd >>= fun stat ->
          if stat.Lwt_unix.LargeFile.st_kind = Lwt_unix.S_REG then
            let size64 = stat.Lwt_unix.LargeFile.st_size in
            if size64 > Int64.of_int Sys.max_string_length then
              Lwt.return (Error (`Storage_error (key, "file too large to process")))
            else
              let size = Int64.to_int size64 in
              let buffer = Bytes.create size in
              Lwt_unix.read fd buffer 0 size >|= fun read_bytes ->
              if read_bytes = size then
                Ok (Bytes.unsafe_to_string buffer)
              else
                Error (`Storage_error (key, Printf.sprintf "could not read %d bytes" size))
          else
            Lwt.return (Error (`Value_expected key)))
        (fun () -> Lwt_unix.close fd))
    (function
      | Unix.Unix_error (Unix.ENOENT, _, _) ->
        Lwt.return (Error (`Not_found key))
      | Unix.Unix_error (e, _, _) ->
        Lwt.return (Error (`Storage_error (key, Unix.error_message e)))
      | e -> Lwt.fail e)

let size_impl base name =
  let fullname = resolve_filename base name in
  Lwt.catch (fun () ->
    Lwt_unix.LargeFile.stat fullname >|= fun stat ->
    match stat.Lwt_unix.LargeFile.st_kind with
      | Lwt_unix.S_REG -> Ok stat.Lwt_unix.LargeFile.st_size
      | _ -> Error `Is_a_directory)
    err_catcher
