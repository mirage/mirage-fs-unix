(*
 * Copyright (c) 2015      Mindy Preston <mindy@somerandomidiot.com>
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

let test_fs = "test_directory"
let empty_file = Mirage_kv.Key.v "empty"
let content_file = Mirage_kv.Key.v "content"
let big_file = Mirage_kv.Key.v "big_file"
let directory = Mirage_kv.Key.v "a_directory"

module FS_impl = FS_unix

let lwt_run f () = Lwt_main.run (f ())

let do_or_fail = Rresult.R.get_ok

let failf fmt = Fmt.kstrf Alcotest.fail fmt
let assert_fail e = failf "%a" FS_unix.pp_error e
let assert_write_fail e = failf "%a" FS_unix.pp_write_error e

let connect_present_dir () =
  FS_impl.connect test_fs >>= fun _fs -> Lwt.return_unit

let clock = lwt_run (fun () -> Pclock.connect ()) ()

let append_timestamp s =
  let now = Ptime.v (Pclock.now_d_ps clock) in
  let str = Fmt.strf "%s-%a" s (Ptime.pp_rfc3339 ~space:false ()) now in
  Mirage_kv.Key.v str

let full_path dirname filename = Mirage_kv.Key.add dirname filename

let just_one_and_is expected read_buf =
  Alcotest.(check string) __LOC__ expected read_buf;
  Lwt.return read_buf

let expect_error_connecting where () =
  Lwt.catch
    (fun () ->
       FS_impl.connect where >>= fun _ ->
       Lwt.fail_with "expected error")
    (fun _ -> Lwt.return_unit)

let size fs key =
  FS_impl.get fs key >|= function
  | Ok data -> Ok (String.length data)
  | Error e -> Error e

let connect_to_empty_string = expect_error_connecting ""

let connect_to_dev_null = expect_error_connecting "/dev/null"

let read_nonexistent_file file () =
  let key = Mirage_kv.Key.v file in
  FS_impl.connect test_fs >>= fun fs ->
  FS_impl.get fs key >>= function
  | Ok _ ->
    failf "read returned Ok when no file was expected. Please make sure there \
           isn't actually a file named %a" Mirage_kv.Key.pp key
  | Error (`Not_found _) ->
    Lwt.return_unit
  | Error e ->
    failf "Unreasonable error response when trying to read a nonexistent file: %a"
      FS_impl.pp_error e

let read_empty_file () =
  FS_impl.connect test_fs >>= fun fs ->
  FS_impl.get fs empty_file >>= function
  | Ok buf when String.length buf = 0 -> Lwt.return_unit
  | Ok _  -> failf "reading an empty file returned some cstructs"
  | Error e ->
    failf "read failed for a present but empty file: %a" FS_impl.pp_error e

let read_big_file () =
  FS_impl.connect test_fs >>= fun fs ->
  size fs big_file >|= do_or_fail >>= fun size' ->
  FS_impl.get fs big_file >|= do_or_fail >>= function
  | buf when String.length buf = size' -> Lwt.return_unit
  | _ -> failf "read returned nothing for a large file"

let size_nonexistent_file () =
  FS_impl.connect test_fs >>= fun fs ->
  let filename = Mirage_kv.Key.v "^#$\000not a file!!!. &;" in
  size fs filename >>= function
  | Ok d -> failf "Got a size of %d for absent file" d
  | Error (`Not_found _) -> Lwt.return_unit
  | Error e -> assert_fail e

let size_empty_file () =
  FS_impl.connect test_fs >>= fun fs ->
  size fs empty_file >|= do_or_fail >>= fun n ->
  Alcotest.(check int) "size of an empty file" 0 n;
  Lwt.return_unit

let size_small_file () =
  FS_impl.connect test_fs >>= fun fs ->
  size fs content_file >|= do_or_fail >>= fun n ->
  Alcotest.(check int) "size of a small file" 13 n;
  Lwt.return_unit

let size_a_directory () =
  FS_impl.connect test_fs >>= fun fs ->
  size fs directory >>= function
  | Error (`Value_expected _) -> Lwt.return_unit
  | Error e -> assert_fail e
  | Ok n -> failf "got size %d on a directory" n

let size_big_file () =
  FS_impl.connect test_fs >>= fun fs ->
  size fs big_file >|= do_or_fail >>= fun size ->
  Alcotest.(check int) __LOC__ 5000 size;
  Lwt.return_unit

let write_not_a_dir () =
  let dirname = append_timestamp "write_not_a_dir" in
  let subdir = "not there" in
  let content = "puppies" in
  let full_path = Mirage_kv.Key.(dirname / subdir / "file") in
  FS_impl.connect test_fs >>= fun fs ->
  FS_impl.set fs full_path content >|= do_or_fail >>= fun () ->
  FS_impl.exists fs full_path >>= function
  | Error e ->
    failf "Exists on an existing file failed %a" FS_impl.pp_error e
  | Ok None ->
    failf "Exists on an existing file returned None"
  | Ok (Some `Dictionary) ->
    failf "Exists on an existing file returned a dictionary"
  | Ok (Some `Value) ->
    FS_impl.get fs full_path >|= do_or_fail >>= fun buf ->
    Alcotest.(check string) __LOC__ content buf;
    Lwt.return_unit

let write_zero_bytes () =
  let dirname = append_timestamp "mkdir_not_a_dir" in
  let subdir = "not there" in
  let full_path = Mirage_kv.Key.(dirname / subdir / "file") in
  FS_impl.connect test_fs >>= fun fs ->
  FS_impl.set fs full_path "" >|= do_or_fail >>= fun () ->
  (* make sure it's size 0 *)
  size fs full_path >>= function
  | Ok n -> Alcotest.(check int) __LOC__ 0 n;
    Lwt.return_unit
  | Error e ->
    failf "write claimed to create a file that the fs then couldn't read: %a"
      FS_impl.pp_error e

let write_contents_correct () =
  let dirname = append_timestamp "write_contents_correct" in
  let full_path = full_path dirname "short_phrase" in
  let phrase = "standing here on this frozen lake" in
  FS_impl.connect test_fs >>= fun fs ->
  FS_impl.set fs full_path phrase >|= do_or_fail >>= fun () ->
  FS_impl.get fs full_path >|= do_or_fail >>=
  just_one_and_is phrase >>= fun _ -> Lwt.return_unit

let write_overwrite_dir () =
  let dirname = append_timestamp "write_overwrite_dir" in
  FS_impl.connect test_fs >>= fun fs ->
  let subdir = Mirage_kv.Key.(dirname / "data") in
  FS_impl.set fs subdir "noooooo" >|= do_or_fail >>= fun () ->
  FS_impl.set fs dirname "noooooo" >>= function
  | Error (`Key_exists _) -> Lwt.return_unit
  | Error e -> assert_write_fail e
  | Ok () -> failf "write overwrote an entire directory! That should not happen!"

let write_big_file () =
  let how_big = 4100 in
  let dirname = append_timestamp "write_big_file" in
  let full_path = full_path dirname "so many bytes!" in
  let zero_cstruct cs =
    let zero c = Cstruct.set_char c 0 '\000' in
    let i = Cstruct.iter (fun _ -> Some 1) zero cs in
    Cstruct.fold (fun b _ -> b) i cs
  in
  let first_page = zero_cstruct (Cstruct.create how_big) in
  Cstruct.set_char first_page 4097 'A';
  Cstruct.set_char first_page 4098 'B';
  Cstruct.set_char first_page 4099 'C';
  FS_impl.connect test_fs >>= fun fs ->
  (* TODO get rid of cstruct *)
  FS_impl.set fs full_path (Cstruct.to_string first_page) >|= do_or_fail >>= fun () ->
  size fs full_path >|= do_or_fail >>= fun sz ->
  let check_chars str a b c =
    Alcotest.(check char) __LOC__ 'A' (String.get str a);
    Alcotest.(check char) __LOC__ 'B' (String.get str b);
    Alcotest.(check char) __LOC__ 'C' (String.get str c)
  in
  Alcotest.(check int) __LOC__ how_big sz;
  FS_impl.get fs full_path >|= do_or_fail >>= fun s ->
  if s = "" then failf "claimed a big file was empty on read"
  else check_chars s 4097 4098 4099; Lwt.return_unit


let populate num depth fs =
  let rec gen_d pref = function
    | 0 -> "foo"
    | x -> Filename.concat (pref ^ (string_of_int x)) (gen_d pref (pred x))
  in
  let rec gen_l acc = function
    | 0 -> acc
    | x -> gen_l (gen_d (string_of_int x) depth :: acc) (pred x)
  in
  (* populate a bit *)
  Lwt_list.iteri_s (fun i x ->
      FS_impl.set fs (append_timestamp ("foo" ^ x ^ (string_of_int i))) "test content"
      >|= do_or_fail >>= fun _ -> Lwt.return_unit
  ) (gen_l [] num)


let destroy () =
  let files = Mirage_kv.Key.to_string (append_timestamp ("/tmp/" ^ test_fs ^ "2")) in
  Lwt_unix.mkdir files 0o755 >>= fun () ->
  let cleanup () = Lwt_unix.rmdir files in
  FS_impl.connect files >>= fun fs ->
  populate 10 4 fs >>= fun () ->
  FS_impl.remove fs Mirage_kv.Key.empty >>= function
  | Error _ -> cleanup () >>= fun () -> failf "create failed"
  | Ok () ->
    FS_impl.list fs Mirage_kv.Key.empty >>= function
    | Ok []   -> Lwt.return_unit
    | Ok _    -> failf "something exists after destroy"
    | Error e -> failf "error %a in listdir" FS_impl.pp_error e

let destroy_a_bit () =
  let files = Mirage_kv.Key.to_string (append_timestamp ("/tmp/" ^ test_fs ^ "3")) in
  Lwt_unix.mkdir files 0o755 >>= fun () ->
  let cleanup () = let _ = Sys.command ("rm -rf " ^ files) in Lwt.return_unit in
  FS_impl.connect files >>= fun fs ->
  populate 10 4 fs >>= fun () ->
  FS_impl.list fs Mirage_kv.Key.empty >>= (function
      | Ok xs -> Lwt.return (List.length xs)
      | Error _ -> failf "error in list") >>= fun files ->
  FS_impl.set fs (Mirage_kv.Key.v "barf") "dummy content" >>= function
  | Error _ -> cleanup () >>= fun () -> failf "create failed"
  | Ok ()   -> FS_impl.remove fs (Mirage_kv.Key.v "barf") >>= (function
      | Error _ -> cleanup () >>= fun () -> failf "destroy failed"
      | Ok ()   -> FS_impl.list fs Mirage_kv.Key.empty >>= (function
          | Ok xs when List.length xs = files -> cleanup ()
          | Ok _ ->
            failf "something wrong in destroy: destroy  followed by create is \
                   not well behaving"
          | Error _ -> failf "error in listdir"))

let () =
  let connect = [
    "connect_to_empty_string", `Quick, lwt_run connect_to_empty_string;
    "connect_to_dev_null", `Quick, lwt_run connect_to_dev_null;
    "connect_present_dir", `Quick, lwt_run connect_present_dir;
  ] in
  let read = [
    "read_nonexistent_file_from_root", `Quick,
    lwt_run (read_nonexistent_file "^$@thing_that_isn't_in root!!!.space");
    "read_nonexistent_file_from_dir", `Quick,
    lwt_run (read_nonexistent_file "not a *dir*?!?/thing_that_isn't_in root!!!.space");
    "read_empty_file", `Quick, lwt_run read_empty_file;
    "read_big_file", `Quick, lwt_run read_big_file;
  ] in
  let destroy = [
    "destroy_file", `Quick, lwt_run destroy;
    "create_destroy_file", `Quick, lwt_run destroy_a_bit
  ] in
  let size = [
    "size_nonexistent_file", `Quick, lwt_run size_nonexistent_file;
    "size_empty_file", `Quick, lwt_run size_empty_file;
    "size_small_file", `Quick, lwt_run size_small_file;
    "size_a_directory", `Quick, lwt_run size_a_directory;
    "size_big_file", `Quick, lwt_run size_big_file;
  ] in
  let listdir = [ ] in
  let write = [
    "write_not_a_dir", `Quick, lwt_run write_not_a_dir;
    "write_zero_bytes", `Quick, lwt_run write_zero_bytes;
    "write_contents_correct", `Quick, lwt_run write_contents_correct;
    "write_overwrite_dir", `Quick, lwt_run write_overwrite_dir;
    "write_big_file", `Quick, lwt_run write_big_file;
  ] in
  Alcotest.run "FS_impl" [
    "connect", connect;
    "read", read;
    "size", size;
    "destroy", destroy;
    "listdir", listdir;
    "write", write;
  ]
