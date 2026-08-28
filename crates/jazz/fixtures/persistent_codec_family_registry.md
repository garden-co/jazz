# Persistent codec-family registry

`persistent_codec_family_registry.json` is the machine-checked inventory for
the storage-settlement compatibility corpus. It deliberately records more than
the top-level manifest profile: a physical row/value/key family can be
authoritative without deserving a separate profile identifier.

Every entry names one boundary (`durable-storage`, `wire-binding-abi`, or
`local-auth-secret`) and must point to:

1. its normative specification or invariant;
2. a committed semantic-to-exact-byte fixture;
3. a malformed/noncanonical rejection receipt; and
4. backend, recovery, or reopen evidence.

When adding an authoritative codec, first decide whether it is a new storage
epoch/profile family or an existing typed-record family. Then add its registry
row and exact tests in the same change. The registry verifier rejects missing
fields, stale pointers, duplicate IDs, and any known epoch-one profile member
that lacks a row. Wire/binding and local-auth-secret entries are listed
separately on purpose: they are compatibility boundaries, but they are not
durable ordered-KV profile IDs.

The registry is an inventory, not a migration mechanism. An incompatible
epoch-one durable change still requires a new storage epoch and an explicit
migration decision.
