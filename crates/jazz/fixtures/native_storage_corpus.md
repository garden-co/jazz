# Native Jazz epoch-1 compatibility corpus

The executable producer is
`node::tests::harness::settlement_baseline_native_jazz_corpus_reopens_and_accepts_mixed_writes`.
It deliberately uses fixed node, row, branch, transaction-time, text, and
byte-scalar inputs, then opens the resulting Jazz root through both production
native adapters under the closed `epoch_1_storage_codec_profile`.

Its settlement-baseline logical-receipt SHA-256 is:

```text
0a81edc17508c40d1a04c52bfc00e92f34da4bf9070d394dc038370701a2779b
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

The currently populated historical families are catalogue genesis, durable
node/schema identities, branch-keyed immutable versions/current projections,
transaction/fate/global-change/merge-head records, and an authenticated
indirect byte tree. Its fixed test-only capabilities exist solely to make this
historical receipt reproducible; product writers continue to mint random
capabilities. The registry additionally names provenance, catalogue pointers,
deletion history, known state, settled result members, and program facts so an
intentionally narrow producer cannot quietly become the final corpus.

The producer's exact logical pack is committed as
`epoch-1-native-jazz-corpus.pack.base64`; it is base64 only to keep its
canonical binary values safe in a text repository. Its SHA-256 is
`e3f4e1cffd6f3c2aec48cddd57bfad38f00b0b285470d13fed0cbf6282af1477`.
It explicitly lists empty authoritative families as well as entries, so an
omitted opened family cannot look the same as an empty one.

The same pinned producer has two backend-specific physical receipts:

- `epoch-1-native-jazz.sqlite.gz.base64` — gzip payload SHA-256
  `50a977b27eda5ff07b416958001f9e91f45ee3a1dec4732af9b9a3aeb54dc0f5`,
  decompressed SQLite SHA-256
  `4f06a04f731b7b8a449d600829c2dd6a7b2fff982f30ec5d37cd93afab89d2f3`.
- `epoch-1-native-jazz-rocksdb.tar.gz.base64` — archive SHA-256
  `edfd22271a49e252b7b909d9693f0d5b0db9cba849aff843bb033879c1a9e8da`.

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
