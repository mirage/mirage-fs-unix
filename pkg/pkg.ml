#!/usr/bin/env ocaml
#use "topfind"
#require "topkg"
open Topkg

let opam =
  let nolint = ["mirage-types-lwt"; "ounit"; "oUnit"] in
  Pkg.opam_file ~lint_deps_excluding:(Some nolint) "opam"

let () =
  Pkg.describe ~opams:[opam] "mirage-fs-unix" @@ fun c ->
  Ok [
    Pkg.mllib ~api:["FS_unix"; "Kvro_fs_unix"] "src/mirage-fs-unix.mllib";
    Pkg.test  "test/test_fs_unix";
  ]
