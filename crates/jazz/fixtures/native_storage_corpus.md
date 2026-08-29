# Native Jazz epoch-1 compatibility corpus

The executable producer is
`node::tests::harness::settlement_baseline_native_jazz_corpus_reopens_and_accepts_mixed_writes`.
It deliberately uses fixed node, row, branch, transaction-time, text, and
byte-scalar inputs, then opens the resulting Jazz root through both production
native adapters under the closed `epoch_1_storage_codec_profile`.

Its settlement-baseline logical-receipt SHA-256 is:

```text
56e86db7698356339de33afbc7416abb38a1bff153340757dc9db383ba6fa0d3
```

The digest is over sorted system and physical application store names and, for
each store, the exact primary-key/value byte pairs in scan order. The physical
application closure includes each permanent table identity's history,
register, global-current, ahead-current, and rejected-version stores.
Direct-record stores use their typed semantic key/value fixture because Groove
intentionally does not expose their raw adapter keys through the public
direct-store facade. The test proves
both adapters produce the same pack, rejects an incomplete stored-codec
profile before touching it, reopens without application writes, writes a new
current-format row, and reopens a third time while preserving the older
history.

A separate planted sensitivity receipt repeats the deterministic producer while
changing only `todos.title`, then while changing only `notes.body`; each change
must alter the digest. This prevents a metadata-only exporter from silently
passing while excluding application rows.

The currently populated historical families are catalogue genesis plus an
active deterministic lineage/lens, durable node/schema identities,
branch-keyed immutable versions/current projections, transaction/fate/global-
change/merge-head records, globally settled deletion history, known-state and
settled result/program facts, and an authenticated indirect byte tree. Its
fixed test-only capabilities exist solely to make this historical receipt
reproducible; product writers continue to mint random capabilities. The
registry additionally tracks the more focused provenance and catalogue codec
fixtures that supply corruption detail beyond this whole-root receipt.

The producer's exact logical pack is committed as
`epoch-1-native-jazz-corpus.pack.base64`; it is base64 only to keep its
canonical binary values safe in a text repository. Its SHA-256 is
`37d21323cc3feca56ca983dc29e7daf39d114efbb05294e4de5a0b146ce52f08`.
It explicitly lists empty authoritative families as well as entries, so an
omitted opened family cannot look the same as an empty one.

The same pinned producer has two backend-specific physical receipts:

- `epoch-1-native-jazz.sqlite.gz.base64` — gzip payload SHA-256
  `15d6328adeb9be044daa07e0032ae0109e78a2dd21a60dbf35368bcaa5fc150a`,
  decompressed SQLite SHA-256
  `a95e7b9b3bc80a2ec24a6976817828cc79db50c9069248ef90670eaf570f9e40`.
- `epoch-1-native-jazz-rocksdb.tar.gz.base64` — archive SHA-256
  `41c1444ec70ecc945444b96b67b1c96857b8b102a6ee0ae5a266332b484c75b5`.

`committed_native_jazz_physical_corpus_reopens_and_accepts_current_writes`
checks each payload before materializing it, inspects SQLite and RocksDB using
their read-only physical APIs, opens the exact logical snapshot through current
Jazz, materializes the full indirect value, writes one new current-format row,
and reopens it. The paired corruption test proves a changed archive is rejected
before any target file or extraction directory is created. SQLite pages and
RocksDB files remain backend-owned bytes, not interchange encodings.

The fixture was produced by the deterministic source in this file's executable
test on the storage-settlement branch. Reproducing it is intentionally an
explicit review action: set `JAZZ_NATIVE_CORPUS_PACK_OUT`,
`JAZZ_NATIVE_CORPUS_SQLITE_OUT`, and `JAZZ_NATIVE_CORPUS_ROCKS_ARCHIVE_OUT`
while running `settlement_baseline_native_jazz_corpus_reopens_and_accepts_mixed_writes`.
Pre-settlement alpha stores remain intentionally unsupported.
