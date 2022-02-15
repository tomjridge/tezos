This is a branch to record various comments on Tezos.

# installing


https://tezos.gitlab.io/introduction/howtoget.html#compiling-with-make

`make build-deps`

NOTE as of 2022-02-15, this will create an _opam switch just for tezos; it is a local switch; so don't use an existing _opam (typically can timeout; OPAMSOLVERTIMEOUT)

You can use opam switch export (emacs: tjr/opam-switch-export) to see installed packages

Then `make` will make everything (remember to `eval $(opam env)`)

To build just a single package (eg lib_context): `dune build src/lib_context`


# deps on irmin

grepping for irmin in dune files:

src/lib_store/dune

src/lib_context/... (lots eg encoding/dune)



# lib_store

See src/lib_store/README.md

See https://tezos.gitlab.io/shell/storage.html

Quotes from that link:

The storage layer is responsible for aggregating blocks (along with their respective
ledger state) and operations (along with their associated metadata). It is composed of two
main parts: the store and the context.

The store also manages the context and handles its initialization, but it is not
responsible to commit contexts on-disk. This is done by the validator component.

The store is initialized using a history mode that can be either Archive, Full or
Rolling. Depending on the chosen history mode, some data will be pruned while the chain is
growing. In Full mode, all blocks that are part of the chain are kept but their associated
metadata below a certain threshold are discarded. In Rolling mode, blocks under a certain
threshold are discarded entirely. Full and Rolling may take a number of additional cycles
to increase or decrease that threshold.

To decide whether a block should be pruned or not, the store uses the latest head’s
metadata that contains the last allowed fork level. This threshold specifies that the
local chain cannot be reorganized below it. When a protocol validation returns a change to
this value, it means that a cycle has completed. Then, the store retrieves all the blocks
from (head-1).last_allowed_fork_level + 1 to head.last_allowed_fork_level, which contain
all the blocks of a completed cycle that cannot be reorganized anymore, and trims the
potential branches in the process to yield a linear history.


The context is a versioned key/value store that associates for each block a view of its
ledger state. The versioning uses concepts similar to Git. The current implementation is
using irmin as backend and abstracted by the lib_context library.

