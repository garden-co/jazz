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
  `jazz.subscription-program-fact-key.v1`,
  `jazz.transaction-durability.v2`
- adapter parameter: `key-order=unsigned-lexicographic`
- SHA-256 of the committed canonical `JSM1` bytes:
  `72683cdf9083aa6fd76f3b523d5c541c93b57b4d502411ade0d8baff4a2aad36`
- receipt: `storage_codec_profile::tests::epoch_one_jazz_profile_has_a_pinned_manifest_receipt`

An omitted, added, duplicate, or substituted ID fails profile admission before
the adapter decodes or mutates ordinary data. After epoch-one freeze, any incompatible inventory change
requires an explicitly versioned codec profile, migration decision, and updated fixture; this is
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
base above plus `jazz.history-version-current.v4`, the linear row-state
history layout (SPEC 2 §2.7.1), and `groove.durable-index.v2`, the compact
secondary-index layout (below). Roots that hold no row history (the server
account registry and catalogue-entry store) keep the base profile unchanged.

- codec registry, in canonical order: the base list with
  `groove.durable-index.v2` inserted first and
  `jazz.history-version-current.v4` inserted after
  `jazz.catalogue.write-pointer.v1` (15 families)
- SHA-256 of the committed canonical `JSM1` bytes (adapter sample `memory`,
  `key-order=unsigned-lexicographic`):
  `5c976ed2a3e64d20c72d230145673be62b1d4b320aef36ccc57acf3d0bdfedf1`
  (14 families without `groove.durable-index.v2`:
  `b0d8e4455d94b9a7e62dc625e5d65ca89403d35481e42681da27709ed87e9c73`)
- receipt: `storage_codec_profile::tests::node_profile_has_a_pinned_manifest_receipt_and_refuses_base_only_roots`

A node root written by the DAG layout (alpha.54 to alpha.57) declares only the
base profile. Manifest admission refuses it with the typed
`groove::storage::Error::UnsupportedStorageCodecs { epoch: 1, missing:
["groove.durable-index.v2", "jazz.history-version-current.v4",
"jazz.transaction-durability.v2"], unknown: [...] }`
before any ordinary key is decoded or written (`tests/storage_format_refusal.rs`).
No migration exists.

The touched-rows transaction record (2026-09-30) replaced
`jazz.history-version-current.v2` with `v3`: history and ahead-current tables
lost their `by_tx` indexes and `jazz_transactions` gained `touched_rows`
(SPEC 2 §2.8). Implicit history `updated_by` (2026-09-30) then replaced `v3`
with `v4`: a history image stores `updated_by` only when it differs from its
transaction's `made_by`, and the touched-row list moved out of
`jazz_transactions` into the node-local `jazz_tx_touched_rows`. Neither v2 nor v3 was in a published release. A v2
or v3 root is refused with `missing: ["groove.durable-index.v2",
"jazz.history-version-current.v4"], unknown: [<its family>]`
(`storage_codec_profile::tests::node_profile_refuses_history_v2_and_v3_roots`;
such a root also predates durability v2 below, so `missing` names
`jazz.transaction-durability.v2` as well).
The v2 manifest SHA-256 was
`1153be8475ab6fd239d109e376663220d349fa9ac1f034b77cbdc8bc38f0631a`; v3 was
`323199b2f7206bebea3eb2bf48859a253ff648d88a95c0232a66e6025516377d`.

## Compact durable-index layout (alpha.60)

`groove.durable-index.v2` is Groove's secondary-index layout (Groove SPEC 2,
"Durable index layout"): each entry key is a LEB128 numeric index id followed by
the concatenated, single-escaped index key parts, and its value is empty (a
unique index stores only the primary-key columns the index lacks). Ids live in
the `\0groove-index-id\0` registry in the same family and are never reused.
The earlier layout prefixed every key with `table\0index\0`, wrapped the key in
a second escaped `Bytes` part and repeated it in the value.

Only node roots declare schema indexes, so the family joins the node profile,
not the base. A node root with the current history family but without it is refused
with `missing: ["groove.durable-index.v2"]`
(`linear_history_root_without_the_durable_index_family_is_refused`). No
migration exists.

## Durability encoding v2 (read tiers, 2026-10-08)

Durability v2 adds `jazz.transaction-durability.v2` to the epoch-one base
(13 families): transaction durability is stored with the compact tags
`None=0`, `Local=1`, `Global=2`. Earlier profiles are rejected without
migration; the physical Groove epoch stays 1. Because the base grows, the
node profile above grows with it, so every earlier node root is refused
naming the families it lacks: a DAG-layout root (alpha.54 to alpha.59) lacks
all three of `groove.durable-index.v2`, `jazz.history-version-current.v4` and
`jazz.transaction-durability.v2`, a read-tiers DAG root lacks the first two,
and an unreleased linear-history root written before durability v2 lacks only
`jazz.transaction-durability.v2`
(`linear_history_root_without_the_durability_v2_family_is_refused`). The
pre-durability-v2 node manifest SHA-256 was
`06e335498e562c06e78207cddd8027752c673d981640c78ef584aa95406775db`
(without `groove.durable-index.v2`:
`73ece466df8d410135a648b0697126d9e3a77b977ecd6aaae0718e48bac3f319`).
