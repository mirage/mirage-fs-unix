all:
	ocaml pkg/pkg.ml build -q --tests true
	ocaml pkg/pkg.ml test -q

clean:
	ocaml pkg/pkg.ml clean
