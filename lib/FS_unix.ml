(*
 * Copyright (c) 2013-2014 Anil Madhavapeddy <anil@recoil.org>
 * Copyright (c) 2014      Thomas Gazagnaire <thomas@gazagnaire.org>
 * Copyright (c) 2014      Hannes Mehnert <hannes@mehnert.org>
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
type block_device_error = unit

type error = [
  | `Not_a_directory of string
  | `Is_a_directory of string
  | `Directory_not_empty of string
  | `No_directory_entry of string * string
  | `File_already_exists of string
  | `No_space
  | `Format_not_recognised of string
  | `Unknown_error of string
  | `Block_device of block_device_error
]
type page_aligned_buffer = Cstruct.t

let string_of_error = function
  | `Not_a_directory x         -> Printf.sprintf "%s is not a directory" x
  | `Is_a_directory x          -> Printf.sprintf "%s is a directory" x
  | `Directory_not_empty x     -> Printf.sprintf "The directory %s is not empty" x
  | `No_directory_entry (x, y) -> Printf.sprintf "The directory %s contains no entry called %s" x y
  | `File_already_exists x     -> Printf.sprintf "The filename %s already exists" x
  | `No_space -> "There is insufficient free space to complete this operation"
  | `Format_not_recognised x   -> Printf.sprintf "This disk is not formatted with %s" x
  | `Unknown_error x           -> Printf.sprintf "Unknown error: %s" x
  | `Block_device x            -> Printf.sprintf "Block device error"

exception Error of error

let (>>|) x f =
  x >>= function
  | `Ok x    -> f x
  | `Error e -> fail (Error e)

type t = {
  base: string
}

let disconnect t =
  return ()

let id {base} = base

let read {base} name off len =
  Fs_common.read_impl base name off len 

let size {base} name =
  Fs_common.size_impl base name

type stat = {
  filename: string;
  read_only: bool;
  directory: bool;
  size: int64;
}

(* all mkdirs are mkdir -p *)
let rec create_directory path =
  let check_type p =
    Lwt_unix.LargeFile.stat path >>= fun stat ->
    match stat.Lwt_unix.LargeFile.st_kind with
    | Lwt_unix.S_DIR -> Lwt.return (`Ok ())
    | _ -> Lwt.return (`Error (`File_already_exists path))
  in
  if Sys.file_exists path then check_type path
  else begin
    create_directory (Filename.dirname path) >>= function
    | `Error e -> Lwt.return (`Error e) (* TODO: this leaks information *)
    | `Ok () ->
      try_lwt
        Lwt_unix.mkdir path 0o755 >>= fun () -> Lwt.return (`Ok ())
      with Unix.Unix_error (ex, _, _) ->
        return (Fs_common.map_error ex path)
  end

let mkdir {base} path =
  let path = Fs_common.resolve_filename base path in
  create_directory path

let command fmt =
  Printf.ksprintf (fun str ->
      let i = Sys.command str in
      if i <> 0 then
        return (`Error (`Unknown_error (str ^ ": exit " ^ string_of_int i)))
      else
        return (`Ok ())
    ) fmt

let format {base} _ =
  assert (base <> "/");
  command "rm -rf %s" base

let destroy {base} path =
  let path = Fs_common.resolve_filename base path in
  command "rm -rf %s" path

let create {base} path =
  let path = Fs_common.resolve_filename base path in
  create_directory (Filename.dirname path) >>= fun _ ->
  command "touch %s" path

let stat {base} path0 =
  let path = Fs_common.resolve_filename base path0 in
  try_lwt
    Lwt_unix.LargeFile.stat path >>= fun stat ->
    let size = stat.Lwt_unix.LargeFile.st_size in
    let filename = Filename.basename path in
    let read_only = false in
    let directory = Sys.is_directory path in
    return (`Ok { filename; read_only; directory; size })
  with Unix.Unix_error (ex, _, _) ->
    return (Fs_common.map_error ex path)

let connect id =
  try_lwt
    match Sys.is_directory id with
    | true -> return (`Ok {base = id})
    | false -> return (`Error (`Not_a_directory id))
  with (Sys_error _) -> return (`Error (`No_directory_entry (id, "")))

let listdir {base} path =
  let path = Fs_common.resolve_filename base path in
  if Sys.file_exists path then (
    let s = Lwt_unix.files_of_directory path in
    let s = Lwt_stream.filter (fun s -> s <> "." && s <> "..") s in
    Lwt_stream.to_list s >>= fun l ->
    return (`Ok l)
  ) else
    return (`Ok [])

let write {base} path off buf =
  let path = Fs_common.resolve_filename base path in
  create_directory (Filename.dirname path) >>= function
  | `Error e -> Lwt.return (`Error e)
  | `Ok () ->
    try_lwt 
      Lwt_unix.(openfile path [O_WRONLY; O_NONBLOCK; O_CREAT] 0o644) >>= fun fd ->
      catch
        (fun () ->
           Lwt_unix.lseek fd off Unix.SEEK_SET >>= fun _seek ->
           let buf = Cstruct.to_string buf in
           let rec aux off remaining =
             if remaining = 0 then
               Lwt_unix.close fd
             else (
               Lwt_unix.write fd buf off remaining >>= fun n ->
               aux (off+n) (remaining-n))
           in
           aux 0 (String.length buf) >>= fun () ->
           return (`Ok ()))
        (fun e ->
           Lwt_unix.close fd >>= fun () ->
           match e with
           | Unix.Unix_error (ex, _, _) -> return (Fs_common.map_error ex path)
           | e -> Lwt.fail e
        )
    with
    | Unix.Unix_error (ex, _, _) -> return (Fs_common.map_error ex path)
