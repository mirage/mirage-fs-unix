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

open Lwt.Infix

type +'a io = 'a Lwt.t

type key = Mirage_kv.Key.t
type value = string

type t = {
  base: string
}

type error = [ Mirage_kv.error | `Storage_error of Mirage_kv.Key.t * string ]

let pp_error ppf = function
  | #Mirage_kv.error as err -> Mirage_kv.pp_error ppf err
  | `Storage_error (key, msg) -> Fmt.pf ppf "storage error for %a: %s"
                                   Mirage_kv.Key.pp key msg

type write_error = [ Mirage_kv.write_error | `Storage_error of Mirage_kv.Key.t * string | `Key_exists of Mirage_kv.Key.t ]

let pp_write_error ppf = function
  | #Mirage_kv.write_error as err -> Mirage_kv.pp_write_error ppf err
  | `Key_exists key -> Fmt.pf ppf "key %a already exists and is a dictionary" Mirage_kv.Key.pp key
  | `Storage_error (key, msg) -> Fmt.pf ppf "storage error for %a: %s"
                                   Mirage_kv.Key.pp key msg
 

let disconnect _ = Lwt.return ()

let get {base} key = FS_common.read_impl base key

(* all mkdirs are mkdir -p *)
let rec create_directory t key =
  let path = FS_common.resolve_filename t.base (Mirage_kv.Key.to_string key) in
  let check_type path =
    Lwt_unix.LargeFile.stat path >>= fun stat ->
    match stat.Lwt_unix.LargeFile.st_kind with
    | Lwt_unix.S_DIR -> Lwt.return (Ok ())
    | _ -> Lwt.return (Error (`Dictionary_expected key))
  in
  if Sys.file_exists path then check_type path
  else begin
    create_directory t (Mirage_kv.Key.parent key) >>= function
    | Error e -> Lwt.return (Error e)
    | Ok () ->
      Lwt.catch (fun () ->
        Lwt_unix.mkdir path 0o755 >>= fun () -> Lwt.return (Ok ())
      )
      (function
        | Unix.Unix_error (e, _, _) -> Lwt.return (Error (`Storage_error (key, Unix.error_message e)))
        | e -> Lwt.fail e
      )
  end

let open_file t key flags =
  let path = FS_common.resolve_filename t.base (Mirage_kv.Key.to_string key) in
  (* create_directory (Filename.dirname path) *)
  create_directory t (Mirage_kv.Key.parent key) >>= function
  | Error e -> Lwt.return (Error e)
  | Ok () ->
    Lwt.catch (fun () -> Lwt_unix.openfile path flags 0o644 >|= fun fd -> Ok fd)
      (function
        | Unix.Unix_error (Unix.ENOSPC, _, _) -> Lwt.return (Error `No_space)
        | Unix.Unix_error (e, _, _) -> Lwt.return (Error (`Storage_error (key, Unix.error_message e)))
        | e -> Lwt.fail e)

let file_or_directory {base} path0 =
  let path = FS_common.resolve_filename base (Mirage_kv.Key.to_string path0) in
  Lwt_unix.LargeFile.stat path >|= fun stat ->
  match stat.Lwt_unix.LargeFile.st_kind with
  | Lwt_unix.S_DIR -> Ok `Dictionary
  | Lwt_unix.S_REG -> Ok `Value
  | _ -> Error (`Storage_error (path0, "not a regular file"))

(* TODO test this *)
let exists t path0 =
  Lwt.catch (fun () -> file_or_directory t path0 >|= function
    | Error e -> Error e
    | Ok x -> Ok (Some x))
    (function
      | Unix.Unix_error (Unix.ENOENT, _, _) -> Lwt.return (Ok None)
      | Unix.Unix_error (e, _, _) -> Lwt.return (Error (`Storage_error (path0, Unix.error_message e)))
      | e -> Lwt.fail e)

let last_modified {base} key =
  let path = FS_common.resolve_filename base (Mirage_kv.Key.to_string key) in
  Lwt.catch (fun () ->
      Lwt_unix.LargeFile.stat path >|= fun stat ->
      let mtime = stat.Lwt_unix.LargeFile.st_mtime in
      match Ptime.of_float_s mtime with
      | None -> Error (`Storage_error (key, "mtime parsing failed"))
      | Some ts -> Ok (Ptime.Span.to_d_ps (Ptime.to_span ts))
    )
    (function
      | Unix.Unix_error (e, _, _) -> Lwt.return (Error (`Storage_error (key, Unix.error_message e)))
      | e -> Lwt.fail e)

let batch _ ?retries:_ret _ = assert false

let digest _ _ = assert false

let connect id =
  try if Sys.is_directory id then
      Lwt.return ({base = id})
    else
      Lwt.fail_with ("Not a directory " ^  id)
  with Sys_error _ -> Lwt.fail_with ("Not an entity " ^ id)

let list t key =
  let path = FS_common.resolve_filename t.base (Mirage_kv.Key.to_string key) in
  Lwt.catch (fun () ->
    let s = Lwt_unix.files_of_directory path in
    let s = Lwt_stream.filter (fun s -> s <> "." && s <> "..") s in
    Lwt_stream.to_list s >>= fun l ->
    Lwt_list.fold_left_s (fun result filename ->
        match result with
        | Error e -> Lwt.return (Error e)
        | Ok files ->
          file_or_directory t (Mirage_kv.Key.add key filename) >|= function
          | Error e -> Error e
          | Ok kind -> Ok ((filename, kind) :: files))
      (Ok []) l)
    (function
      | Unix.Unix_error (Unix.ENOENT, _, _) -> Lwt.return (Error (`Not_found key))
      | Unix.Unix_error (Unix.ENOTDIR, _, _) -> Lwt.return (Error (`Dictionary_expected key))
      | Unix.Unix_error (e, _, _) -> Lwt.return (Error (`Storage_error (key, Unix.error_message e)))
      | e -> Lwt.fail e)

let rec remove t key =
  let path = FS_common.resolve_filename t.base (Mirage_kv.Key.to_string key) in
  Lwt.catch (fun () ->
      file_or_directory t key >>= function
      | Error e -> Lwt.return (Error e)
      | Ok `Value ->
        Lwt_unix.unlink path >|= fun () ->
        Ok ()
      | Ok `Dictionary ->
        list t key >>= function
        | Error e -> Lwt.return (Error e)
        | Ok files ->
          Lwt_list.fold_left_s (fun result (name, _) ->
              match result with
              | Error e -> Lwt.return (Error e)
              | Ok () -> remove t (Mirage_kv.Key.add key name))
            (Ok ()) files >>= function
          | Error e -> Lwt.return (Error e)
          | Ok () -> 
             if not Mirage_kv.Key.(equal empty key) 
             then Lwt_unix.rmdir path >|= fun () -> Ok ()
             else Lwt.return (Ok ()))
    (function
      | Unix.Unix_error (Unix.ENOENT, _, _) -> Lwt.return (Error (`Not_found key))
      | e -> Lwt.fail e)

(* TODO test this *)
let set t key value =
  exists t key >>= function
  | Ok (Some `Dictionary) -> Lwt.return (Error (`Key_exists key))
  | _ -> 
    remove t key >>= function
    | Error (`Dictionary_expected e) -> Lwt.return (Error (`Dictionary_expected e))
    | Error (`Storage_error e) -> Lwt.return (Error (`Storage_error e))
    | Ok () | Error (`Not_found _) ->
      open_file t key Lwt_unix.([O_WRONLY; O_NONBLOCK; O_CREAT]) >>= function
      | Error e -> Lwt.return (Error e)
      | Ok fd ->
        Lwt.catch
          (fun () ->
             let buf = Bytes.unsafe_of_string value in
             let rec write_once off len =
               if len = 0 then Lwt_unix.close fd
               else
                 Lwt_unix.write fd buf off len >>= fun n_written ->
                 if n_written = len + off then
                   Lwt_unix.close fd
                 else
                   write_once (off + n_written) (len - n_written)
             in
             write_once 0 (String.length value) >|= fun () ->
             Ok ())
          (function
            | Unix.Unix_error (Unix.ENOSPC, _, _) -> Lwt.return (Error `No_space)
            | Unix.Unix_error (e, _, _) -> Lwt.return (Error (`Storage_error (key, Unix.error_message e)))
            | e -> Lwt.fail e)
