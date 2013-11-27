# raft-clj

A Clojure library that implements the Raft consensus algorithm.

See [In Search of an Understandable Consensus Algorithm](https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf) by Diego Ongaro and John Ousterhout for more information.


This is a work in progress.


## TODO

Implement configuration change mechanism and verify demo functionality.

Also, see docs/intro.md for some additional notes.


## Usage

Requires [Leiningen](https://github.com/technomancy/leiningen).


The demo can be run with:

    $ lein run


See some information about command-line arguments:


    $ lein run -- --help



## License

Copyright © 2013 John Weaver

Distributed under the Eclipse Public License, the same as Clojure.
