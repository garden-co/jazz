# Jazz storage profile receipts

## Independent root profiles

The physical storage epoch remains 1. Each independently persisted root
requires exactly one Jazz-owned format marker in addition to Groove's base:
`groove.large-value.v2`, `groove.ordered-chunk-storage.v2`, and
`groove.ordered-kv.v1`.

| Root                | Jazz marker                 | Additional Groove families                        |
| ------------------- | --------------------------- | ------------------------------------------------- |
| Client, relay, Core | `jazz.node-storage.v1`      | `groove.dictionary.v1`, `groove.durable-index.v2` |
| Server catalogue    | `jazz.catalogue-storage.v1` | None                                              |
| Account registry    | `jazz.account-storage.v1`   | None                                              |

Node format v1 covers branch keys, catalogue state, linear row history,
transactions (including the view-scoped cardinality flag), durability, touched-row
lists, and all other node-owned records. Catalogue format v1 covers entry
keys, entry envelopes and persisted catalogue payloads. Account format v1
covers journal keys and command records. None inherits another Jazz profile.

Node format v1 also covers the alpha.60 transaction record layout
(`jazz.tx-record.v2`, SPEC 2 §2.8): `jazz_transactions` with a flags byte and
the kind-specific rejection payload, `jazz_pending_transactions`,
`jazz_tx_provenance` and `jazz_history_horizon`. `groove.dictionary.v1` is
Groove's dictionary of interned column values (Groove SPEC 2, "Interned
columns and dictionaries"): every stored author column (`made_by` in
`jazz_transactions` and `jazz_rejected_transactions`, `created_by`/`updated_by`
in history, global-current and ahead-current rows) holds a 4-byte code into
one node-local `authors` dictionary; reads, queries, policies and the wire see
full authors. A node root without it is refused with `missing:
["groove.dictionary.v1"]` (`node_root_without_the_dictionary_family_is_refused`).

Individual byte families keep their named receipts in
`persistent_codec_family_registry.json`; only entries with `profile` are
manifest members. This separates encoding documentation from root admission.

## Byte receipts

`storage_codec_profile::tests::root_profiles_have_pinned_manifest_bytes`
pins all three exact `JSM1` manifests (memory adapter version 1, no parameters):

- node: `264c7ee05973986d1746dcb106f2031b7a816bda4f471ea1764b5501428c7969`
- catalogue: `92b82965f6a8d36cd85299b6f8c1af345ddec20b52f09b24b27cdde68cc792db`
- account: `815ca7a4044e7888ff605cc7a7f7ed00c5e8f36b9bfdd7c9f131e8cdc9008e1b`

## Compatibility

Manifest admission compares complete inventories, not independently negotiated
features. Previous per-family Jazz profiles, missing/future formats, and roots
opened as the wrong kind are refused before ordinary data is decoded or
mutated, with the typed `UnsupportedStorageCodecs { missing, unknown }` error
(also for roots that still declare the retired `groove.large-value.v1` and
`groove.ordered-chunk-storage.v1`). Older binaries likewise reject these
consolidated profiles. No migration or fallback reader is introduced, and the
physical storage epoch stays 1.

This is not wire compatible, although the wire protocol version is unchanged
(v6) in this change:

- `JVRR` row payload bytes change (large-value scalar tags, descriptor and
  edit layouts, record layouts). #4071 made this change under the `JVRR\x02`
  marker; the current row blob is `JVRR\x03` (#4019's per-message descriptor
  table, SPEC 16).
- Large-value scalar tags `Primitive`/`Chunked` move from 2/3 to 0/1, so
  `BranchKey` column-value bytes change (Groove SPEC 9, "Scalar tag history").
- Catalogue schema bytes always use version 2 and lineage publications always
  use the counted `jazz-schema-lineage-publication-v2` identity, so
  single-predecessor lineage publication IDs change.
- React Native foreground row payloads carry the new scalar tags, but the
  native relay ABI stays 2.

Until the alpha.60 release bumps the wire version (and the native relay ABI)
together with this storage cleanup, a peer built before this change and a peer
built with it both advertise v6, pass the Hello, and then fail with opaque
decode or content-ID errors instead of a typed version refusal. This change
must not ship in a release without that bump. Details and receipts:
`dev/proofs/storage-format-cleanup-compatibility.md`.

Historical published/epoch-one/pre-linear fixtures remain immutable refusal
evidence. Current native and browser fixtures are regenerated through their
production-adapter producers. Node, catalogue and account reopen tests cover
their independent current profiles. The browser's replica identity and lease
metadata remain independent physical metadata and are unchanged.
