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

Linear row-state history (2026-09-29) retired `jazz.history-version-current.v1`,
`jazz.contribution-provenance.v1` and `jazz.merge-heads.v1`: no current code
writes or reads them, and a root written with them is refused at manifest
admission. It added `jazz.history-version-current.v2`, the one member of the
`jazz-node-root` profile that every row-holding root declares on top of the
epoch-one `jazz-root` base, and the non-profile
`jazz.subscription-watermark.v1` direct record store.

The touched-rows transaction record (2026-09-30) retired the unreleased
`jazz.history-version-current.v2` (history and ahead-current `by_tx` indexes)
in favour of `jazz.history-version-current.v3`, whose `jazz_transactions`
records list the rows each transaction touched. Implicit history
`updated_by` (2026-09-30) then retired the unreleased `v3` in favour of
`jazz.history-version-current.v4`: a history image stores `updated_by` only
when it differs from its transaction's `made_by`, and a transaction's
`touched_rows` list longer than 32 rows moves to `jazz_tx_touched_rows` so
transaction records stay small.

The compact durable-index layout (alpha.60) added `groove.durable-index.v2`
to the `jazz-node-root` profile: numeric index ids, single-escaped keys and
empty index values. Node roots written before it lack it and are
refused at manifest admission. The same change moved the non-profile Jazz
physical class layout to `groove.jazz-physical-class.v2` (marker
`class-cf-v2`, the `indices` class stored without the logical-name frame);
a `class-cf-v1` marker is refused as an older layout.
