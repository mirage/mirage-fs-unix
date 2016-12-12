all:
	ocaml pkg/pkg.ml build -q --tests true

test:
	ocaml pkg/pkg.ml test -q

clean:
	ocaml pkg/pkg.ml clean
