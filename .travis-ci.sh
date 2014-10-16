# OPAM packages needed to build tests.
OPAM_PACKAGES="lwt cstruct.1.4.0 mirage-types"

ppa=avsm/ocaml41+opam11
echo "yes" | sudo add-apt-repository ppa:$ppa
sudo apt-get update -qq
sudo apt-get install -qq ocaml ocaml-native-compilers camlp4-extra opam
export OPAMYES=1

opam init git://github.com/ocaml/opam-repository >/dev/null 2>&1
opam install ${OPAM_PACKAGES}

eval `opam config env`
ocaml setup.ml -configure
make build
