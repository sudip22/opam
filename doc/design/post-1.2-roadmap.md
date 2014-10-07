Wanted and ongoing features:


## Portability

- Rewrite parallel command engine.
  I've been wanting to do that for a while, for now it forks then calls system(). It would be much cleaner -- and portable to Windows -- to directly open sub-processes and use waitpid. Also, there are currently some issues like scrambled output when using `--jobs`. There's some wiring to do to properly initialise and finalise everything in the main process.

- Native system manipulation (cp, rm, curl...).
  These are mostly done through calls to the shell or external programs. It's not very pretty but quite pragmatic actually... until we want to run on Windows without Cygwin. Anyway, this wouldn't be the end of portability issues.

- Test & fix on Windows.
  Cygwin, then, maybe, native.

- Packages on Windows.
  Locate common issues and attempt to find generic fixes.

- Allow direct use of more solvers or solver servers


## Agnosticity

- Isolate OCaml-specific stuff.
  E.g. specific opam-file variables. There shouldn't be too much of it, 

- Compilers as packages.
  This solves quite a few issues and feature requests, and I already had a POC one year ago. It requires:
    - a special status of "base" packages that are immovable and shouldn't present any implementation difficulty
    - a repo rewrite (I already had this)
    - a hook to generate and update a package for the system compiler


## Features

- Specify and implement hooks

- Find a nicer way to share dev repos / undoable "pinning sources"

- Per-switch remotes

- Multi-switch packages

- Support for (automatic generation of) binary packages

- Nicer ocamlfind interaction
