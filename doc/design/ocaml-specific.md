OCaml specific stuff in OPAM (and how to get rid of it)

* "remove without dl" hack for package only doing "ocamlfind remove" at uninstall
  -> can be removed, now there is another way (package flag)
* field "ocaml-version"
  -> can be removed, now there is another way ('available' field)
* a bunch of predifined variables: ocaml-version, ocaml-native-*, etc.
  -> should be moved to a plugin
* specific init rules: .ocamlinit, variables CAML_LD_LIBRARY_PATH, OCAML_TOPLEVEL_PATH
  -> plugin
* .install fields: stubs, toplevel are kind of specific
  -> not really a problem, but should make sure we cover generic use-cases
* default repository
  -> to configure from plugin
* opam file template: refers to ocamlfind
  -> make generic or move to plugin
* safety check on ocamlfind specific variables that may interfere with builds
  -> add to plugin (or remove)
* references in doc, messages & --help
  -> review and fix
* system compiler handling
  -> compiler-as-packages will already need to find a more generic handling for this: move to repo if possible, or could also be handled from the ocaml plugin
