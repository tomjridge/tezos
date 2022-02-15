# lib_context (tezos-context)


Some subdirs, with other libs:

- sigs (no deps)
- encoding (no deps)
- helpers (encoding, sigs)
- mem (sigs, encoding, helpers)
- test (for tezos-context)

And then tezos-context (this dir) depends on sigs, helpers, encoding

See `tezos_context.ml` for the intf - just two modules public, context and context_dump.

https://tezos.gitlab.io/api/api-inline.html for whole ocamldoc
