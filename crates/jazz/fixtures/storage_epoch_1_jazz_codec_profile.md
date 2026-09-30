# Jazz storage epoch 1 codec-profile receipt

This fixture freezes the closed codec inventory supplied whenever Jazz opens a
durable ordered-KV root. Groove receives these IDs as opaque manifest metadata;
the identifiers below are the complete Jazz-owned byte families reachable from
that root at the epoch-one settlement baseline.

- epoch: `1`
- adapter sample: `memory`, format version `1`
- codec registry, in canonical order:
  `groove.large-value.v1`, `groove.ordered-chunk-storage.v1`,
  `groove.ordered-kv.v1`, `jazz.branch-key.v1`,
  `jazz.catalogue.activation.v1`, `jazz.catalogue.bootstrap-ready.v1`,
  `jazz.catalogue.lens.v1`, `jazz.catalogue.lineage.v1`, `jazz.catalogue.physical-mapping.v1`,
  `jazz.catalogue.schema.v1`, `jazz.catalogue.write-pointer.v1`,
  `jazz.subscription-program-fact-key.v1`
- adapter parameter: `key-order=unsigned-lexicographic`
- SHA-256 of the committed canonical `JSM1` bytes:
  `a3e89ed15b6b2b243fb15c3eef650d843398cf081ecf3be73f650e741349fe96`
- receipt: `storage_codec_profile::tests::epoch_one_jazz_profile_has_a_pinned_manifest_receipt`

An omitted, added, duplicate, or substituted ID fails profile admission before
the adapter decodes or mutates ordinary data. After epoch-one freeze, any incompatible inventory change
requires a new storage epoch, migration decision, and updated fixture; this is
not a per-adapter `Bytes` compatibility exception.

The browser IndexedDB adapter additionally stores `storage-manifest`/
`replica-node-v1`: one random exact 16-byte `NodeUuid` for that physical
replica. It is created atomically with a fresh browser epoch manifest and
validated before Jazz opens, but is deliberately outside this shared codec
profile and its `JSM1` checksum: it identifies a physical transaction issuer,
whereas this fixture identifies a common decode contract. The browser physical
receipt proves same-replica reopen stability and distinct values for independent
stores with the same logical name.

The pre-freeze #2578 cleanup removes the two dormant result codec IDs. The
required-family count changes from 14 to 12; source payload codecs remain
unchanged. Old roots advertising those retired families fail real current
manifest admission. No compatibility profile is selected to bypass this check.

## Node-root profile (linear row-state history, 2026-09-29)

A root that stores Jazz rows (Core, relay and client node stores on RocksDB,
SQLite and IndexedDB) opens with `node_storage_codec_profile()`: the epoch-one
base above plus `jazz.history-version-current.v2`, the linear row-state
history layout (SPEC 2 §2.7.1). Roots that hold no row history (the server
account registry and catalogue-entry store) keep the base profile unchanged.

- codec registry, in canonical order: the base list with
  `jazz.history-version-current.v2` inserted after
  `jazz.catalogue.write-pointer.v1` (13 families)
- SHA-256 of the committed canonical `JSM1` bytes (adapter sample `memory`,
  `key-order=unsigned-lexicographic`):
  `1153be8475ab6fd239d109e376663220d349fa9ac1f034b77cbdc8bc38f0631a`
- receipt: `storage_codec_profile::tests::node_profile_has_a_pinned_manifest_receipt_and_refuses_base_only_roots`

A node root written by the DAG layout (alpha.54 to alpha.57) declares only the
base profile. Manifest admission refuses it with the typed
`groove::storage::Error::UnsupportedStorageCodecs { epoch: 1, missing:
["jazz.history-version-current.v2"], unknown: [...] }` before any ordinary key
is decoded or written (`tests/storage_format_refusal.rs`). No migration exists.

## Row-author aliases (2026-09-30)

The node-root profile adds `jazz.author-alias.v1` (SPEC 2 §2.2): physical
content rows store `created_by` / `updated_by`, and `jazz_transactions` stores
`made_by`, as a 4-byte little-endian `U32` `AuthorAlias`; the `jazz_authors`
table maps each alias to the exact `RowAuthor` record bytes. The exact row
bytes are pinned by
`node::tests::harness::author_alias_codec_v1_pins_physical_bytes`.

- codec registry, in canonical order: the node-root list above with
  `jazz.author-alias.v1` inserted after `groove.ordered-kv.v1` (14 families)
- SHA-256 of the committed canonical `JSM1` bytes (adapter sample `memory`,
  `key-order=unsigned-lexicographic`):
  `13a1e05bd9f954d92a0af43c1916c493d50da6863421d0562082722cf6da6faf`
  (the 13-family node-root checksum above is superseded)
- receipts:
  `storage_codec_profile::tests::node_profile_has_a_pinned_manifest_receipt_and_refuses_base_only_roots`,
  `storage_codec_profile::tests::node_profile_refuses_a_pre_alias_linear_history_root`

A linear-history root written before aliasing declares
`jazz.history-version-current.v2` but not `jazz.author-alias.v1`. Manifest
admission refuses it with `UnsupportedStorageCodecs { epoch: 1, missing:
["jazz.author-alias.v1"], unknown: [] }` before any record is decoded
(`tests/storage_format_refusal.rs`). A DAG-layout root now reports both
node-root families as missing. No migration exists.
