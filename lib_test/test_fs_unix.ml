open Lwt

let test_fs = "test_output"

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
                                   here *)

let () =
  let connect = [ 
    "connect_to_empty_string", `Quick, lwt_run connect_to_empty_string;
    "test_connect", `Quick, lwt_run test_connect;
  ] in
  let read = [ ] in
  let format = [ ] in
  let create = [ ] in
  let mkdir = [ ] in
  let destroy = [ ] in
  let stat = [ ] in
  let listdir = [ ] in
  let write = [ ] in
  Alcotest.run "FS_unix" [
    "connect", connect;
    "format", format;
    "create", create;
    "read", read;
    "mkdir", mkdir;
    "destroy", destroy;
    "stat", stat;
    "listdir", listdir;
    "write", write;
  ]
