open Lwt

let test_fs = "_tests/test_directory"
let empty_file = "empty"
let content_file = "content"

let lwt_run f () = Lwt_main.run (f ())

let connect_or_fail () =
  FS_unix.connect test_fs >>= function
  | `Error _ -> OUnit.assert_failure "Couldn't connect to test fs; all tests will fail"
  | `Ok fs -> Lwt.return fs

let test_connect () =
  connect_or_fail () >>= fun _fs -> Lwt.return_unit

(* use the empty string as a proxy for "dirname that can't possibly already be there" *)
let connect_to_empty_string () =
  FS_unix.connect "" >>= function
  | `Ok fs -> OUnit.assert_failure "connect let us make an FS under empty string
  directory"
  | `Error _ -> Lwt.return_unit (* TODO: not sure which error is appropriate
                                   here, but we should definitely be getting
                                   *some* kind of error or the types should
                                   reflect that this will always succeed *)

let read_nonexistent_file file () =
  connect_or_fail () >>= fun fs ->
  FS_unix.read fs file 0 1 >>= function
    (* the only vaguely reasonable choices for an error here are
       `No_directory_entry (which is a bit odd because we're asking for
       something at the fs root) or maybe `Unknown_error, or at the outside
       `Block_device *)
  | `Ok _ ->
    OUnit.assert_failure (Printf.sprintf "read returned `Ok for something that shouldn't have
    been there. Please make sure there isn't actually a file named %s present" file)
  | `Error `No_space | `Error (`Directory_not_empty _) 
  | `Error (`Is_a_directory _) | `Error (`Not_a_directory _) 
  | `Error (`File_already_exists _) -> 
    OUnit.assert_failure "Unreasonable error response when trying to read a nonexistent file"
  | `Error (`Unknown_error s) ->
    let chastisement = Printf.sprintf "reading a nonexistent file returned
    `Unknown_error: %s; please make the error nicer" s in
    OUnit.assert_failure chastisement
  | `Error (`Format_not_recognised _) | `Error (`Block_device _) ->
    (* these would be quite odd for FS_unix, but they're not deserving of the level
       of scorn above *)
    OUnit.assert_failure "low-level when trying to test nonexistent file read"
  | `Error (`No_directory_entry (_dirname, basename)) ->
    (* from the implementation, it's clear that the first item in the tuple is
       the name with which we invoked FS_unix.connect, but that's not obvious
       from the documentation *)
    (* it's not clear to me that including (and therefore exposing) the name
       with which the fs was created is useful, so refusing to test it *)
    OUnit.assert_equal ~msg:"does the error content make sense?" basename file;
    Lwt.return_unit

let read_empty_file () =
  connect_or_fail () >>= fun fs ->
  FS_unix.read fs empty_file 0 1 >>= function
  | `Ok [] -> Lwt.return_unit
  | `Ok bufs -> 
    OUnit.assert_failure "reading an empty file returned some cstructs"
  | `Error (`No_directory_entry _) ->
    OUnit.assert_failure (Printf.sprintf "read failed for a present but empty file; please make
      sure %s is present in the test filesystem" empty_file)
  | `Error e -> OUnit.assert_failure (Printf.sprintf "Not the right error: %s"
                                        (FS_unix.string_of_error e))

let read_zero_bytes () =
  connect_or_fail () >>= fun fs ->
  FS_unix.read fs content_file 0 0 >>= function
  | `Ok [] -> Lwt.return_unit
  | `Ok bufs -> 
    OUnit.assert_failure "reading zero bytes from a non-empty file returned some cstructs"
  | `Error (`No_directory_entry _) ->
    OUnit.assert_failure (Printf.sprintf "read failed for a present file; please make
      sure %s is present in the test filesystem" content_file)
  | `Error e -> OUnit.assert_failure (Printf.sprintf "Not the right error: %s"
                                        (FS_unix.string_of_error e))

let read_at_offset () =
  connect_or_fail () >>= fun fs ->
  (* we happen to know that content_file is 13 bytes in size. *)
  FS_unix.read fs content_file 1 12 >>= function
  | `Ok [] -> OUnit.assert_failure "read returned an empty list for a non-empty file"
  | `Error e -> OUnit.assert_failure (FS_unix.string_of_error e)
  | `Ok (buf :: []) ->
    OUnit.assert_equal ~printer:(fun a -> a) "ome content" (Cstruct.to_string buf);
    Lwt.return_unit
  | `Ok bufs -> OUnit.assert_failure "got *way* too much back from reading a short file at offset 1"

let read_at_offset_past_eof () =
  connect_or_fail () >>= fun fs ->
  FS_unix.read fs content_file 50 10 >>= function
  | `Ok [] -> Lwt.return_unit
  | `Ok _ -> OUnit.assert_failure "read returned content when we asked for an offset past EOF"
  | `Error e -> OUnit.assert_failure (FS_unix.string_of_error e)

let size_nonexistent_file () =
  connect_or_fail () >>= fun fs ->
  let filename = "^#$\000not a file!!!. &;" in
  FS_unix.size fs filename >>= function
  | `Ok d -> 
    OUnit.assert_failure (Printf.sprintf "Got a size of %s for absent file"
                            (Int64.to_string d))
  | `Error (`No_directory_entry (_, basename)) -> 
    OUnit.assert_equal filename basename; 
    Lwt.return_unit
  | `Error e -> OUnit.assert_failure (FS_unix.string_of_error e)

let size_empty_file () =
  connect_or_fail () >>= fun fs ->
  FS_unix.size fs empty_file >>= function
  | `Error e -> OUnit.assert_failure (FS_unix.string_of_error e)
  | `Ok n -> OUnit.assert_equal ~msg:"size of an empty file" 
               ~printer:Int64.to_string (Int64.zero) n;
    Lwt.return_unit

let size_small_file () =
  connect_or_fail () >>= fun fs ->
  FS_unix.size fs content_file >>= function
  | `Error e -> OUnit.assert_failure (FS_unix.string_of_error e)
  | `Ok n -> OUnit.assert_equal ~msg:"size of a small file"
               ~printer:Int64.to_string (Int64.of_int 13);
    Lwt.return_unit

let () =
  let connect = [ 
    "connect_to_empty_string", `Quick, lwt_run connect_to_empty_string;
    "test_connect", `Quick, lwt_run test_connect;
  ] in
  let read = [ 
    "read_nonexistent_file_from_root", `Quick, 
    lwt_run (read_nonexistent_file "^$@thing_that_isn't_in root!!!.space");
    "read_nonexistent_file_from_dir", `Quick, 
    lwt_run (read_nonexistent_file "not a *dir*?!?/thing_that_isn't_in root!!!.space");
    "read_empty_file", `Quick, lwt_run read_empty_file;
    "read_zero_bytes", `Quick, lwt_run read_zero_bytes;
    "read_at_offset", `Quick, lwt_run read_at_offset;
    "read_at_offset_past_eof", `Quick, lwt_run read_at_offset_past_eof;
    (*
    "read_big_file", `Quick, lwt_run read_big_file;
     *)
  ] in
  let format = [ ] in
  let create = [ ] in
  let mkdir = [ ] in
  let destroy = [ ] in
  let size = [ 
    "size_nonexistent_file", `Quick, lwt_run size_nonexistent_file;
    "size_empty_file", `Quick, lwt_run size_empty_file;
    "size_small_file", `Quick, lwt_run size_small_file;
    (*
    "size_big_file", `Quick, lwt_run size_big_file;
       *)
  ] in
  let listdir = [ ] in
  let write = [ ] in
  Alcotest.run "FS_unix" [
    "connect", connect;
    "format", format;
    "create", create;
    "read", read;
    "mkdir", mkdir;
    "destroy", destroy;
    "size", size;
    "listdir", listdir;
    "write", write;
  ]
