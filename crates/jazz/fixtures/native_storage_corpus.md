# Native Jazz epoch-1 compatibility corpus

The executable producer is
`node::tests::harness::settlement_baseline_native_jazz_corpus_reopens_and_accepts_mixed_writes`.
It deliberately uses fixed node, row, branch, transaction-time, text, and
byte-scalar inputs, then opens the resulting Jazz root through both production
native adapters under the closed `epoch_1_storage_codec_profile`.

Its settlement-baseline logical-receipt SHA-256 is:

```text
0194e6e7ad2cfdea7650ae9e3c2a50f8ad6429c8d63116cbee3e34a844aa5727
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
`d30a5eb83b9ea6efedaaed691ab514b2fa662419e170c556df755e8c85a3439e`.
It explicitly lists empty authoritative families as well as entries, so an
omitted opened family cannot look the same as an empty one.

The same pinned producer has two backend-specific physical receipts:

- `epoch-1-native-jazz.sqlite.gz.base64` — gzip payload SHA-256
  `717c4f2d7e5ddfeef91aba014c47b0340148c9352b240e0eca84368769f0b039`,
  decompressed SQLite SHA-256
  `c6f6aab28154359d800e7ca81739d257b4456f53c2a9b416128afeec67c32356`.
- `epoch-1-native-jazz-rocksdb.tar.gz.base64` — archive SHA-256
  `e930e931b74425f788f84f5b2776b8dcb4043fb9b8c7391385571b8bf53653d6`.

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
