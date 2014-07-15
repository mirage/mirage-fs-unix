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

let connect id =
  (* TODO verify base directory exists *)
  return (`Ok { base=id })

let disconnect t =
  return ()

let id {base} = base

let read {base} name off len =
  Fs_common.read_impl base name off len >|= function
   | `Error _ -> `Error (`No_directory_entry (base, name))
   | `Ok data -> `Ok data

let size {base} name =
  Fs_common.size_impl base name >|= function
   | `Error _ -> `Error (`No_directory_entry (base, name))
   | `Ok data -> `Ok data

type stat = {
  filename: string;
  read_only: bool;
  directory: bool;
  size: int64;
}

let rec create_directory path : unit Lwt.t =
  if Sys.file_exists path then return_unit
  else (
    create_directory (Filename.dirname path) >>= fun () ->
    if Sys.file_exists path then return_unit
    else
      catch
        (fun () -> Lwt_unix.mkdir path 0o755)
        (fun _  -> return_unit)
  )

let mkdir {base} path =
  match Fs_common.check_filename base path with
  | None -> return (`Error (`No_directory_entry (base, path)))
  | Some path -> create_directory path >|= fun () -> `Ok ()

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
  match Fs_common.check_filename base path with
  | None   -> return (`Error (`No_directory_entry (base, path)))
  | Some p -> command "rm -rf %s" p

let create {base} path =
  match Fs_common.check_filename base (Filename.dirname path) with
  | None -> return (`Error (`No_directory_entry (base, path)))
  | Some path -> create_directory path >>= fun () ->
                 match Fs_common.check_filename base (Filename.dirname path) with
                 | None      -> return (`Error (`No_directory_entry (base, path)))
                 | Some file -> command "touch %s" file

let stat {base} path0 =
  match Fs_common.check_filename base path0 with
  | None -> return (`Error (`No_directory_entry (base, path0)))
  | Some path ->
     try_lwt
       Lwt_unix.LargeFile.stat path >>= fun stat ->
       let size = stat.Lwt_unix.LargeFile.st_size in
       let filename = Filename.basename path in
       let read_only = false in
       let directory = Sys.is_directory path in
       return (`Ok { filename; read_only; directory; size })
     with exn ->
       return (`Error (`No_directory_entry (base, path0)))

let listdir {base} path =
  match Fs_common.check_filename base path with
  | None -> return (`Error (`No_directory_entry (base, path)))
  | Some path ->
     if Sys.file_exists path then (
       let s = Lwt_unix.files_of_directory path in
       let s = Lwt_stream.filter (fun s -> s <> "." && s <> "..") s in
       Lwt_stream.to_list s >>= fun l ->
       return (`Ok l)
     ) else
       return (`Ok [])

let write {base} path off buf =
  match Fs_common.check_filename base (Filename.dirname path) with
  | None -> return (`Error (`No_directory_entry (base, path)))
  | Some path -> create_directory path >>= fun () ->
                 match Fs_common.check_filename base path with
                 | None -> return (`Error (`No_directory_entry (base, path)))
                 | Some path ->
                    Lwt_unix.(openfile path [O_WRONLY; O_NONBLOCK; O_CREAT; O_TRUNC] 0o644) >>= fun fd ->
                    catch
                      (fun () ->
                       Lwt_unix.lseek fd off Unix.SEEK_SET >>= fun _ ->
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
                       return (`Error (`Unknown_error (Printexc.to_string e))))
