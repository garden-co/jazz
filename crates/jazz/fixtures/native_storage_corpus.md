# Native Jazz epoch-1 compatibility corpus

The executable producer is
`node::tests::harness::settlement_baseline_native_jazz_corpus_reopens_and_accepts_mixed_writes`.
It deliberately uses fixed node, row, branch, transaction-time, text, and
byte-scalar inputs, then opens the resulting Jazz root through both production
native adapters under the closed `epoch_1_storage_codec_profile`.

Its settlement-baseline logical-receipt SHA-256 is:

```text
9ad43563145a771423c5bbfabd7d38b7b69c2a8e935c7d5d581d5744231755c4
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
active deterministic lineage/lens and current-write pointer, durable node/schema identities,
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
`1f9cc421ea72a0066c4305ac38916c3b605b352ce4b6f205bb28f5ba0967e361`.
It explicitly lists empty authoritative families as well as entries, so an
omitted opened family cannot look the same as an empty one.

The same pinned producer has two backend-specific physical receipts:

- `epoch-1-native-jazz.sqlite.gz.base64` — gzip payload SHA-256
  `4c713b250eec00b8a6774e33869f5e1ed16e88624424bcb11675c5030ddbc9f9`,
  decompressed SQLite SHA-256
  `4ba479e28c13f3c6233ab0acf65285bb503c6446083966cf72cc5ccba20f23f9`.
- `epoch-1-native-jazz-rocksdb.tar.gz.base64` — archive SHA-256
  `1477e75cb48aa05e347a354b4b4d0edd4d31fde455f7b589390ade2605b0c1f3`.

`committed_native_jazz_physical_corpus_reopens_and_accepts_current_writes`
checks each payload before materializing it, inspects SQLite and RocksDB using
their read-only physical APIs, opens the exact logical snapshot through current
Jazz, materializes the full indirect value, writes one new current-format row,
and reopens it. The paired corruption test proves a changed archive is rejected
before any target file or extraction directory is created. SQLite pages and
RocksDB files remain backend-owned bytes, not interchange encodings.

## Pinned producer provenance

The checked-in epoch-1 fixtures were produced from commit
`7e205805cfb684e595609818137c54ab50351d8f` using the closed
`epoch_1_storage_codec_profile`. The deterministic authority namespace is
`4a` repeated 16 times; the producer node is `c0`, historical todo is `c1`,
its branch selector is `c2`, the historical note is `c3`, the current-format
note is `c4`, the staged byte upload is `c5`, and the deleted note is `c6`.
The source schema has `todos(branch_id, title, attachment)` and `notes(body)`;
the active descendant adds `notes.genre` with default `"instrumental"` and is
current-write revision 1.

Reproduction is an explicit review action. From the repository root, run:

```sh
JAZZ_NATIVE_CORPUS_PACK_OUT=/tmp/epoch-1-native-jazz-corpus.pack \
JAZZ_NATIVE_CORPUS_SQLITE_OUT=/tmp/epoch-1-native-jazz.sqlite \
JAZZ_NATIVE_CORPUS_ROCKS_ARCHIVE_OUT=/tmp/epoch-1-native-jazz-rocksdb.tar.gz \
dev/t --exact node::tests::harness::settlement_baseline_native_jazz_corpus_reopens_and_accepts_mixed_writes
```

The producer intentionally writes candidate outputs before the pinned checksum
assertion, so a deliberate format change leaves reviewable candidates even
though the command exits non-zero. Encode the pack and Rocks archive with
base64; gzip the SQLite file deterministically (`gzip -n -9`) before base64.
Then update all three checksums above together with the executable constants.
Pre-settlement alpha stores remain intentionally unsupported.
