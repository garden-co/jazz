# Native Jazz epoch-1 compatibility corpus

The executable producer is
`node::tests::harness::settlement_baseline_native_jazz_corpus_reopens_and_accepts_mixed_writes`.
It deliberately uses fixed node, row, branch, transaction-time, text, and
byte-scalar inputs, then opens the resulting Jazz root through both production
native adapters under the closed `epoch_1_storage_codec_profile`.

Its settlement-baseline logical-receipt SHA-256 is:

```text
33fdfd9f425d64092e4955a5a43d60b879ed4d997d3948ae894c9aba3276922c
```

The digest is over sorted system and physical application store names and, for
each store, the exact primary-key/value byte pairs in scan order. The physical
application closure includes each permanent table identity's history,
register, global-current, ahead-current, and rejected-version stores.
Direct-record stores use their typed semantic key/value fixture because Groove
intentionally does not expose their raw adapter keys through the public
direct-store facade. The receipt also includes the canonical metadata records
from Groove's `__groove_large_values` family: the seeded indirect attachment's
exact root and node identities, their nonempty lifecycle/reference records,
and the reclaim lifecycle entry. It rejects an unclassified metadata key and
proves a completed local seed leaves no unfinished `install/` recovery marker.
The raw `chunk/` byte-plane entry deliberately remains outside the
backend-neutral pack: it is an adapter-owned installation receipt/blob plane;
the separate physical SQLite and RocksDB fixtures retain and reopen it. The test proves
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
exact covered-input/source-coverage facts, and an authenticated indirect byte tree. Its
fixed test-only capabilities exist solely to make this historical receipt
reproducible; product writers continue to mint random capabilities. The
registry additionally tracks the more focused provenance and catalogue codec
fixtures that supply corruption detail beyond this whole-root receipt.

The producer's exact logical pack is committed as
`epoch-1-native-jazz-corpus.pack.base64`; it is base64 only to keep its
canonical binary values safe in a text repository. Its SHA-256 is
`fc28ce3fa5eee206f375c6dccb3ad8eaacd953b083a5d7739788f3e92125bfbd`.
It explicitly lists empty authoritative families as well as entries, so an
omitted opened family cannot look the same as an empty one.

The same pinned producer has two backend-specific physical receipts:

- `epoch-1-native-jazz.sqlite.gz.base64` — gzip payload SHA-256
  `0436f97b2b8bb04ee286b1ce9a7e1866bdd40115e0878223135bb0e700c5c3a8`,
  decompressed SQLite SHA-256
  `9cf200ef662e18a0f841b9a9ff6606528b026de2ddde5c66703d762e92f457ac`.
- `epoch-1-native-jazz-rocksdb.tar.gz.base64` — archive SHA-256
  `c59d02314c1dc58b69dc51904ecf062ff004fef8056b853d8cb071dd484e9ab7`.

This regeneration replaces the two settled-state records with fixed,
domain-separated digest keys and typed Groove-record values: `jazz_settled_program_facts`
keeps its full semantic fact in the value, while `jazz_settled_result_members`
keeps the full member receipt in the value. The logical pack and both physical
artifacts therefore change together. The deterministic producer/reopen receipt
proves the new records survive fresh SQLite and RocksDB roots before promotion.

`committed_native_jazz_physical_corpus_reopens_and_accepts_current_writes`
checks each payload before materializing it, inspects SQLite and RocksDB using
their read-only physical APIs, opens the exact logical snapshot through current
Jazz, materializes the full indirect value, writes one new current-format row,
and reopens it. The paired corruption test proves a changed archive is rejected
before any target file or extraction directory is created. SQLite pages and
RocksDB files remain backend-owned bytes, not interchange encodings.

The deterministic contract is the semantic/logical pack only. Backend-owned
SQLite pages and RocksDB files are deliberately _not_ expected to reproduce
byte-for-byte: page layout, compaction, and filesystem metadata belong to the
backend. `gzip -n` merely makes the checked-in SQLite wrapper stable enough to
review; it does not make SQLite bytes a format. A regeneration creates its
staging roots internally (the requested paths are publication destinations
only), then produces reviewable candidate artifacts and independently
copies/unpacks each into a
fresh root and runs the complete historical reopen/mixed-write receipt against
those candidates. Promotion therefore requires explicit review of all new
physical checksums as well as the logical-pack checksum.

The staging check is deliberately an accidental-alias guard for a trusted
maintainer filesystem: it rejects dot/root/symlink aliases and any staged
regular file physically linked to the live SQLite image or any live RocksDB
member. It is not a hostile concurrent-filesystem or TOCTOU defense; replacing
parents concurrently with regeneration is out of scope. Requested outputs are
create-new only. Regeneration never overwrites an existing file (including an
alias); delete the old candidate or choose a new path deliberately.

## Pinned producer provenance

The checked-in epoch-1 fixtures were regenerated by the exact source-closure
storage transition using the closed
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

The producer intentionally publishes candidate outputs before the pinned checksum
assertion, so a deliberate format change leaves reviewable candidates even
though the command exits non-zero. It first validates every candidate by
opening a fresh materialized SQLite copy and freshly unpacked RocksDB root with
the full historical receipt. Each requested output must be a fresh path: the
publisher refuses to overwrite an existing artifact. Encode the pack and Rocks archive with base64;
gzip the SQLite file deterministically (`gzip -n -9`) before base64. Then
update the logical-pack checksum and any physical checksums whose reviewed
candidate actually changes, together with the executable constants. A
logical-pack coverage change may leave physical checksums unchanged when the
existing artifacts already contain the newly audited bytes; record that
fresh-root verification explicitly. Pre-settlement alpha stores remain
intentionally unsupported.

## Inactive output persistence removal (#2578)

The current producer no longer registers `jazz_settled_result_members`. Its
previous pack contained only the empty family declaration; the live producer
removes exactly that line and preserves every active entry byte. Historical
physical blobs and their checksums above remain unchanged. They now prove rejection of the retired required-codec profile under real
current admission. New current-profile physical fixtures prove positive reopen;
no old manifest is normalized or admitted through a historical compatibility profile. See the [cleanup proof](../../../dev/proofs/inactive-result-persistence-cleanup.md)
for exact current hashes and retained storage/runtime inventory.
