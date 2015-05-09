open Lwt

let test_fs = "_tests/test_directory"
let empty_file = "empty"
let content_file = "content"
let directory = "a_directory"

let lwt_run f () = Lwt_main.run (f ())

let connect_or_fail () =
  FS_unix.connect test_fs >>= function
  | `Error _ -> OUnit.assert_failure "Couldn't connect to test fs; all tests will fail"
  | `Ok fs -> Lwt.return fs

let connect_present_dir () =
  connect_or_fail () >>= fun _fs -> Lwt.return_unit

(* use the empty string as a proxy for "dirname that can't possibly already be there" *)
let expect_error_connecting where () =
  FS_unix.connect "" >>= function
  | `Ok fs -> OUnit.assert_failure 
                (Printf.sprintf "connect let us make an FS at %s" where)
  | `Error _ -> Lwt.return_unit (* TODO: not sure which error is appropriate
                                   here, but we should definitely be getting
                                   *some* kind of error or the types should
                                   reflect that this will always succeed *)

let assert_fail e = OUnit.assert_failure (FS_unix.string_of_error e)

let connect_to_empty_string = expect_error_connecting ""

let connect_to_dev_null = expect_error_connecting "/dev/null"

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
  | `Error e -> assert_fail e

let read_zero_bytes () =
  connect_or_fail () >>= fun fs ->
  FS_unix.read fs content_file 0 0 >>= function
  | `Ok [] -> Lwt.return_unit
  | `Ok bufs -> 
    OUnit.assert_failure "reading zero bytes from a non-empty file returned some cstructs"
  | `Error (`No_directory_entry _) ->
    OUnit.assert_failure (Printf.sprintf "read failed for a present file; please make
      sure %s is present in the test filesystem" content_file)
  | `Error e -> assert_fail e

let read_at_offset () =
  connect_or_fail () >>= fun fs ->
  (* we happen to know that content_file is 13 bytes in size. *)
  FS_unix.read fs content_file 1 12 >>= function
  | `Ok [] -> OUnit.assert_failure "read returned an empty list for a non-empty file"
  | `Error e -> assert_fail e
  | `Ok (buf :: []) ->
    OUnit.assert_equal ~printer:(fun a -> a) "ome content" (Cstruct.to_string buf);
    Lwt.return_unit
  | `Ok bufs -> OUnit.assert_failure "got *way* too much back from reading a short file at offset 1"

let read_at_offset_past_eof () =
  connect_or_fail () >>= fun fs ->
  FS_unix.read fs content_file 50 10 >>= function
  | `Ok [] -> Lwt.return_unit
  | `Ok _ -> OUnit.assert_failure "read returned content when we asked for an offset past EOF"
  | `Error e -> assert_fail e

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
  | `Error e -> assert_fail e

let size_empty_file () =
  connect_or_fail () >>= fun fs ->
  FS_unix.size fs empty_file >>= function
  | `Error e -> assert_fail e
  | `Ok n -> OUnit.assert_equal ~msg:"size of an empty file" 
               ~printer:Int64.to_string (Int64.zero) n;
    Lwt.return_unit

let size_small_file () =
  connect_or_fail () >>= fun fs ->
  FS_unix.size fs content_file >>= function
  | `Error e -> assert_fail e
  | `Ok n -> OUnit.assert_equal ~msg:"size of a small file"
               ~printer:Int64.to_string (Int64.of_int 13) n;
    Lwt.return_unit

let size_a_directory () = 
  connect_or_fail () >>= fun fs ->
  FS_unix.size fs directory >>= function
  | `Error (`Is_a_directory location) -> 
    OUnit.assert_equal ~printer:(fun a -> a) directory location; Lwt.return_unit
  | `Error e -> assert_fail e
  | `Ok n -> OUnit.assert_failure 
               (Printf.sprintf "got size %s on a directory" (Int64.to_string n))

let mkdir_already_empty_directory () =
  connect_or_fail () >>= fun fs ->
  FS_unix.mkdir fs directory >>= function
    (* TODO: the behaviour should really be a consistent *one* of these, 
       but either is credible, at least if considering only their name *)
  | `Error (`Is_a_directory _) | `Error (`File_already_exists _) -> Lwt.return_unit
  | `Error e -> assert_fail e
  | `Ok () -> OUnit.assert_failure 
                "mkdir indicates no error when directory already present at requested path"

let mkdir_over_file () =
  connect_or_fail () >>= fun fs ->
  FS_unix.mkdir fs empty_file >>= function
  | `Error (`File_already_exists _) -> Lwt.return_unit
  | `Error e -> assert_fail e
  | `Ok () ->
    (* really?  We're already in trouble, but let's see how bad it is *)
    FS_unix.stat fs empty_file >>= function
    | `Error e -> OUnit.assert_failure "mkdir reported success in making a
    directory over an existing file, and now the location isn't even stat-able"
    | `Ok s ->
      let open FS_unix in
      match s.directory with
      | true -> OUnit.assert_failure "mkdir made a dir over top of an existing file"
      | false -> 
        OUnit.assert_failure "mkdir falsely reported success making a directory over an existing file"

let mkdir_over_directory_with_contents () =
  connect_or_fail () >>= fun fs ->
  let tempdir = "mkdir_over_directory_with_contents" ^ (string_of_float
                                                          (Clock.time ())) in
  FS_unix.mkdir fs tempdir >>= function `Error e -> assert_fail e | `Ok () ->
    FS_unix.mkdir fs (tempdir ^ "/cool tapes") >>= function
    | `Error e -> assert_fail e | `Ok () ->
      FS_unix.mkdir fs tempdir >>= function
      | `Error (`Is_a_directory _) | `Error (`File_already_exists _) -> Lwt.return_unit
      | `Error e -> assert_fail e
      | `Ok () -> (* did we clobber the subdirectory that was here? *)
        FS_unix.stat fs (tempdir ^ "/cool tapes") >>= function
        | `Error (`No_directory_entry _) -> 
          OUnit.assert_failure "mkdir silently clobbered an existing directory and all of
          its contents.  That is bad.  Please fix it."
        | `Error e -> assert_fail e
        | `Ok s -> 
          OUnit.assert_failure 
            "mkdir claimed to clobber an existing directory, although it seems to actually have noop'd"

let mkdir_in_path_not_present () =
  let not_a_thing = "%%#@*%#@  $ /my awesome directory!!!" in
  connect_or_fail () >>= fun fs ->
  FS_unix.mkdir fs not_a_thing >>= function
  | `Ok () -> OUnit.assert_failure "reported success making a subdir of a dir that doesn't exist"
  | `Error (`No_directory_entry _) -> Lwt.return_unit
  | `Error e -> assert_fail e

let mkdir_credibly () = 
  let dirname = "mkdir_credibly-" ^ (string_of_float (Clock.time ())) in
  connect_or_fail () >>= fun fs ->
  FS_unix.mkdir fs dirname >>= function
  | `Error e -> assert_fail e
  | `Ok () -> 
    (* really? *)
    FS_unix.stat fs dirname >>= function
    | `Error `No_directory_entry _ -> 
      OUnit.assert_failure "mkdir claimed success, but stat didn't see a dir there"
    | `Error e -> assert_fail e
    | `Ok s ->
      let open FS_unix in
      OUnit.assert_equal true s.directory;
      OUnit.assert_equal dirname s.filename;
      Lwt.return_unit

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
    "read_zero_bytes", `Quick, lwt_run read_zero_bytes;
    "read_at_offset", `Quick, lwt_run read_at_offset;
    "read_at_offset_past_eof", `Quick, lwt_run read_at_offset_past_eof;
    (*
    "read_big_file", `Quick, lwt_run read_big_file;
     *)
  ] in
  let create = [ ] in
  let destroy = [ ] in
  let size = [ 
    "size_nonexistent_file", `Quick, lwt_run size_nonexistent_file;
    "size_empty_file", `Quick, lwt_run size_empty_file;
    "size_small_file", `Quick, lwt_run size_small_file;
    "size_a_directory", `Quick, lwt_run size_a_directory;
    (*
    "size_big_file", `Quick, lwt_run size_big_file;
     *)
  ] in
  let mkdir = [ 
    "mkdir_credibly", `Quick, lwt_run mkdir_credibly; 
    "mkdir_already_empty_directory", `Quick, lwt_run mkdir_already_empty_directory;
    "mkdir_over_file", `Quick, lwt_run mkdir_over_file;
    "mkdir_over_directory_with_contents", `Quick, lwt_run mkdir_over_directory_with_contents;
    "mkdir_in_path_not_present", `Quick, lwt_run mkdir_in_path_not_present;
  ] in
  let listdir = [ ] in
  let write = [ (*
    "write_not_a_dir", `Quick, lwt_run write_not_a_dir;
    "write_zero_bytes", `Quick, lwt_run write_zero_bytes;
    "write_at_offset_within_file", `Quick, lwt_run write_at_offset_within_file;
    "write_causing_truncate", `Quick, lwt_run write_causing_truncate;
    "write_at_offset_is_eof", `Quick, lwt_run write_at_offset_is_eof;
    "write_at_offset_beyond_eof", `Quick, lwt_run write_at_offset_beyond_eof;
    "write_contents_correct", `Quick, lwt_run write_contents_correct;
    "write_overwrite_dir", `Quick, lwt_run write_overwrite_dir;
                   *)
  ] in
  let format = [ ] in
  Alcotest.run "FS_unix" [
    "connect", connect;
    "read", read;
    "size", size;
    "mkdir", mkdir;
    "destroy", destroy;
    "format", format;
    "create", create;
    "listdir", listdir;
    "write", write;
  ]
