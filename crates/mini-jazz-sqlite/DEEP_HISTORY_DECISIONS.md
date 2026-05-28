# Deep History Implementation Decision Log

Timebox start: Wed May 27 22:52:41 PDT 2026
Timebox target end: Thu May 28 04:52:41 PDT 2026

## Wed May 27 22:53:30 PDT 2026

Decision: implement the first sealed accepted-history block format as an lz4-compressed compact `Bundle` payload, rather than immediately building a bespoke binary columnar codec.

Why: the compact bundle format already dictionary-encodes repeated ids and preserves exact logical records. Storing that as a block lets us prove the storage movement, indexing, export, and read semantics first. Once those are stable, the block payload can become more columnar without changing the table/index boundary.

Scope impact: the first vertical slice should add `history_blocks`, compact old accepted open rows into compressed block payloads, and teach exports/historical paths to decode those blocks. Rejected blocks and a more specialized per-column codec can follow once the accepted path is real.

## Wed May 27 22:58:24 PDT 2026

Decision: keep no-block export ordering exactly on the old code path. Only sort/dedupe merged history and tx records when a table actually has sealed blocks.

Why: two existing policy/lens tests caught that even harmless-looking tx/history reordering can change validation outcomes. The block path needs deterministic merge order, but the ordinary path should preserve current behavior until compaction is involved.

Scope impact: sealed-block export has an explicit merge/sort step; ordinary export remains structurally unchanged.

## Wed May 27 22:59:25 PDT 2026

Decision: compact transaction rows opportunistically, not absolutely. A sealed tx row can be deleted from `jazz_tx` only if no current row, open history row, rejection, awaiting-dependency state, or implicit-read successor still needs its physical `tx_num`.

Why: `tx_num` is still the open-store relational key for current projections and implicit previous-local-epoch reads. Deleting every tx in a sealed block would require a larger rewrite of those paths. Opportunistic deletion gives immediate metadata savings while preserving the current operational model.

Scope impact: the block payload is the authoritative historical source for deleted tx metadata. Open tx rows remain where they are still operationally referenced.

## Wed May 27 23:04:44 PDT 2026

Decision: add freelist/live-byte visibility to storage stats rather than treating unchanged SQLite file size as compaction failure.

Why: deleting thousands of ordinary history/tx rows moves pages to SQLite's freelist. That space is reusable by future writes but the database file does not shrink without VACUUM/auto-vacuum maintenance. The benchmark needs to distinguish allocated file bytes from live allocated pages.

Scope impact: benchmark comparisons should include live database bytes or freelist bytes for compaction experiments. A later maintenance API can decide when to VACUUM durable files.

## Wed May 27 23:10:31 PDT 2026

Decision: keep the first public historical read API narrow: one row on main accepted history at a global epoch.

Why: this proves the block lookup/decode path without prematurely claiming full branch/query snapshot support. Branch-local overlays, policy-filtered historical queries, and rejected diagnostic reads need separate semantics.

Scope impact: `read_row_at_global_epoch` can use sealed accepted blocks plus open history. Broader snapshot APIs remain future work.

## Wed May 27 23:12:25 PDT 2026

Decision: accepted compaction must always retain the current visible history row in open history, even when the requested hot tail is zero.

Why: current reads use the current projection, but rebuild/replay still needs an ordinary open head until projection rebuild is block-aware. Deleting the visible head would make compaction appear correct until a rebuild or restart path needed history as the source of truth.

Scope impact: the first compactor can seal old history aggressively but treats the current visible tx as operational state.

## Wed May 27 23:16:38 PDT 2026

Decision: represent accepted and rejected sealed history as different block kinds in one physical `history_blocks` table, rather than creating parallel accepted/rejected block tables.

Why: the block payload, lz4 codec, tx-range index, and point lookup mechanics are identical. A `block_kind` keeps accepted and rejected history logically separate while letting us reuse the same storage and decoder paths. If rejected history later needs a substantially different payload, the kind boundary is still enough to route it.

Scope impact: accepted export and point-read paths must filter to accepted blocks. Rejected diagnostics can decode only rejected blocks, and old rejected rows can leave `history_open` without polluting accepted snapshots.

## Wed May 27 23:18:18 PDT 2026

Decision: add batched SQLite commit policy as a stretch goal in the sealed-history RFC, separate from block compaction.

Why: compaction reduces the steady-state byte cost of deep history, but the write workloads also pay a large per-logical-write SQLite transaction cost. Grouping multiple logical Jazz transactions into one SQLite commit should improve import and high-frequency write throughput while preserving per-logical-tx ids and listener events.

Scope impact: this remains design-only for now. The block work should not hide whether wins come from fewer bytes or fewer durable commit boundaries, so future benchmarks should keep those dimensions separate.

## Wed May 27 23:21:17 PDT 2026

Decision: archived transaction helper APIs should decode sealed block payloads before callers need to know whether a tx is still open.

Why: `transaction_info` alone is not enough for sync/debug tooling. Read/write dependency helpers are part of the transaction metadata surface, and sealing tx rows should not create a split-brain API where some metadata works and adjacent metadata silently disappears.

Scope impact: when ordinary `jazz_tx_*` views return no rows for a tx id, the helpers fall back to block-local `history` and `reads` records. This still preserves the fast open path for recent transactions.

## Wed May 27 23:24:28 PDT 2026

Decision: query-scoped sync exports should splice in sealed accepted history for only the row ids already selected or repaired by that query.

Why: after compaction, a matching current row may have old versions and transaction records only in sealed blocks. Full-table export already decodes those blocks, but query-scoped export is the normal client path and should remain semantically equivalent without degrading into table replication.

Scope impact: query export stays row-scoped. It decodes accepted blocks for the selected/repair row nums, merges their reads and tx metadata, and sorts only when sealed records were actually added so ordinary no-block export order remains unchanged.

## Wed May 27 23:25:17 PDT 2026

Decision: defer block-aware projection rebuild until the replay path can be shaped around logical records, and first make rejected compaction operationally symmetric with accepted compaction.

Why: `projection.rs` currently rebuilds by replaying physical history SQL rows. Sealed blocks decode to logical bundle records, so teaching rebuild to use them cleanly is more than a local query patch. Table-level rejected compaction is smaller, directly useful for maintenance, and keeps rejected history from becoming the next open-row leak.

Scope impact: continue keeping visible accepted heads open for now. Add a table-wide rejected compaction API before attempting a larger logical replay refactor.

## Wed May 27 23:27:00 PDT 2026

Decision: benchmark block compaction should call the table-level compaction API, even in single-row workloads.

Why: the maintenance boundary we want long-term is table/row-family compaction, not callers manually knowing a hot row id. Using the table API in benchmarks keeps the benchmark closer to the real maintenance shape while producing the same single-row behavior for the current canonical scenarios.

Scope impact: the benchmark note now reports "table history block compaction". This is not expected to materially change current numbers because each canonical workload still has one hot row.

## Wed May 27 23:28:45 PDT 2026

Decision: prototype batched writes as "many logical Jazz transactions inside one SQLite transaction", starting with a narrow batched-update API.

Why: this tests the stretch-goal premise without changing transaction identity or visibility semantics. Each logical update still receives its own tx id, history row, read set, write set, and sync representation; only the durable SQLite commit boundary is shared.

Scope impact: `update_rows_batched` is intentionally narrow and does not yet provide scheduler/count/time policies. It is enough to benchmark whether reducing commit count matters for append/edit streams before designing the production API.

## Wed May 27 23:30:59 PDT 2026

Decision: expose batched logical updates in the deep-history benchmark behind `MINI_JAZZ_DEEP_HISTORY_WRITE_BATCH_SIZE`.

Why: batching is a write-throughput optimization, not a storage compaction optimization. The benchmark should let us turn it on independently for Base, Block, and future experiments so we can tell whether wins come from fewer SQLite commits or fewer persisted bytes.

Scope impact: benchmark sampling flushes any pending batch before listener/export measurement. This preserves end-to-end listener correctness while allowing normal non-sampled writes to share SQLite commits.

## Wed May 27 23:33:39 PDT 2026

Decision: add a payload-free history block manifest API before attempting block-native sync.

Why: sync and maintenance need a cheap way to discover which blocks exist, what row/table/epoch range they cover, and how large they are without decompressing payloads. This is also the right inspection boundary for future "do I already have this block?" negotiation.

Scope impact: `history_block_manifests(table)` exposes accepted and rejected block metadata with row ids and byte counts. It deliberately does not expose payload bytes or decode logical records.

## Wed May 27 23:34:23 PDT 2026

Decision: add an all-table maintenance compaction API that compacts accepted and rejected history separately for every table in the schema.

Why: callers should not need to know which table or row is currently hot to get the storage benefits. The RFC describes `history_open` as an operational hot tail and blocks as colder maintenance output, so the natural application boundary is "compact this database with these retention knobs."

Scope impact: keep the accepted and rejected streams separate but aggregate their stats into one return value for observability.

## Wed May 27 23:38:24 PDT 2026

Decision: rejected tx metadata can leave `jazz_tx` only after every open history row for that tx has been sealed.

Why: rejected multi-row transactions can write multiple rows. Compacting one row must not delete the shared tx/rejection metadata while sibling rejected rows still live in ordinary history tables, or those rows become relationally stranded.

Scope impact: rejected compaction now mirrors accepted compaction's conservative metadata deletion rule: seal row history first, delete tx metadata only when no open history row still references it.
Wed May 27 23:41:40 PDT 2026

## Block-Native History Payload Exchange

Decision: expose history blocks as raw lz4 payload records with manifest metadata, and import them idempotently by rebuilding the local tx index from the decoded block contents. This keeps sync experiments from expanding sealed history into ordinary rows while preserving existing Bundle decoding as the semantic validation step.

Scope impact: this is not yet a full protocol; it is a local/runtime API that lets benchmarks and future sync code move sealed blocks as blocks.
Wed May 27 23:45:50 PDT 2026

## Block-Aware Projection Rebuild

Decision: teach the runtime rebuild path to replay the latest accepted sealed history version per row/branch after the ordinary open-history rebuild. Open history remains authoritative when it contains a newer visible version; sealed blocks fill gaps left by aggressive compaction or block-native import.

Scope impact: this is a runtime-level bridge for the prototype, not yet a shared projection engine. It deliberately decodes whole blocks because rebuild is already an offline recovery path.
Wed May 27 23:48:21 PDT 2026

## Visible Head History Can Be Sealed

Decision: allow accepted compaction with `hot_tail = 0` to seal the current visible history row. The current projection row remains the hot read copy, and the tx row remains open while current points at it; block-aware rebuild can recover the projection if current is cleared.

Scope impact: this reduces open history rows further without forcing current reads to decode blocks or deleting transaction metadata still referenced by current rows.
Wed May 27 23:50:05 PDT 2026

## Block Import Does Not Recreate Archived Tx Rows

Decision: raw history block import should populate node ids and block tx-range indexes, but should not recreate ordinary `jazz_tx` rows for every archived transaction. Transaction helpers can resolve through the block index and decode tx metadata from the block payload.

Scope impact: rebuilding current projection may still recreate the visible transaction row needed by current rows, but cold archived txs remain block-native on import.
Wed May 27 23:55:30 PDT 2026

## Explicit SQLite Space Reclaim

Decision: keep history compaction and SQLite file reclamation separate. Compaction should free rows/pages quickly inside the normal write path; a separate explicit reclaim operation can checkpoint/truncate WAL and run `VACUUM` when the caller is willing to pay the latency.

Scope impact: benchmarks can report both live database bytes after compaction and total file bytes after reclaim, instead of hiding vacuum cost inside every compaction call.
Thu May 28 00:03:12 PDT 2026

## Tx Ranges In Block Export Manifests

Decision: include node-local tx ranges in exported history block records and use those ranges to build `history_block_tx_index` on import without decoding the full block payload. Payload decoding remains the on-demand path for exact transaction info and historical rows.

Scope impact: block import becomes a manifest operation plus one payload insert; trusted/local block exchange gets faster, while future untrusted block exchange will need a separate integrity/hash validation layer.
Thu May 28 00:07:20 PDT 2026

## Payload Hashes For Block Comparison

Decision: include SHA-256 payload hashes in block manifests/exports. Fast block import can validate the payload bytes against the manifest hash without decoding the block, and future sync can compare manifests by range plus hash.

Scope impact: this adds a small dependency and a cheap per-block hash calculation, but avoids making block comparison depend on decoding the lz4 payload.
Thu May 28 00:09:43 PDT 2026

## All-Table Block Discovery

Decision: expose all-table history block manifest/export helpers in addition to per-table helpers. Block-aware sync and maintenance should be able to discover sealed history without first knowing which user table has blocks.

Scope impact: the helpers are still explicit runtime APIs, not automatic sync behavior; they reuse the same per-block manifests, tx ranges, hashes, and payload bytes.
Thu May 28 00:12:28 PDT 2026

## Columnar Block Payload V2

Decision: switch newly sealed history blocks from compact Bundle JSON to a columnar JSON payload compressed with lz4. The decoder still returns the same logical Bundle, but the physical block stores tx/read/history fields as parallel arrays so the format matches the RFC direction and avoids repeated object keys.

Scope impact: keep decoding support for the existing v1 `bundle-json-lz4` blocks so earlier checkpoints and tests remain readable. The v2 format is still JSON inside lz4, not the final binary/delta-varint encoding.

## Thu May 28 00:21:08 PDT 2026 - Manifest-Selected Block Sync

Decision: Treat history block manifests as a peer-comparable inventory, but keep SQLite `block_id` local-only. A remote block is considered present when kind, table, row id, epoch range, codec, format version, byte counts, and payload hash match.

Scope impact: add APIs for computing missing remote block manifests and exporting only matching local blocks. This gives sync a first-class block delta path without decoding cold history or reopening compacted rows.

## Thu May 28 00:24:30 PDT 2026 - Persisted Block Hashes

Decision: Store `payload_sha256` in `history_blocks` and bump the prototype storage format to 12. Hashing remains mandatory at import boundaries, but local inventory reads should not deserialize or hash compressed payload blobs.

Scope impact: manifest listing and block existence checks can use metadata columns and an inventory index. This keeps the block-native sync path shaped like a real delta protocol rather than an O(payload bytes) scan.

## Thu May 28 00:27:05 PDT 2026 - Exact Block Export

Decision: Make manifest-selected export query exact block identities instead of exporting all local blocks and filtering in memory. Duplicate requested manifests are deduped before querying.

Scope impact: this keeps block sync scalable with the number of requested missing blocks, not total local cold history, and preserves `block_id` as a receiver-local detail.

## Thu May 28 00:29:17 PDT 2026 - Block-Native Table Delta

Decision: Add a table delta export that leaves sealed history as raw history blocks and sends only open history rows in the ordinary bundle. The receiver imports missing blocks, rebuilds from sealed state if needed, then applies the open bundle.

Scope impact: this creates the first end-to-end sync shape for compacted history without decoding cold blocks back into row-version bundles on the sender. It still uses existing bundle semantics for hot/open history.

## Thu May 28 00:30:54 PDT 2026 - Benchmark Block Delta Path

Decision: Benchmark block-native export/import as open bundle plus sealed block transfer, including receiver block import, projection rebuild, and open bundle apply. Keep the old full bundle bytes separately so we can see how expensive compatibility export remains.

Scope impact: future canonical Block numbers should represent the intended compacted-history sync shape rather than a raw-block-only microprobe.

## Thu May 28 00:36:36 PDT 2026 - No Rebuild For Block Delta Apply

Decision: Do not require a projection rebuild between importing sealed blocks and applying the open-history bundle in the block-native table delta path. Open history exports already contain full row values, so applying the hot tail can materialize current state directly.

Scope impact: block-native receiver timing should include block import plus open bundle apply, not an avoidable full sealed-block projection rebuild. Historical reads and explicit rebuild still decode sealed blocks when requested.

## Thu May 28 00:40:11 PDT 2026 - All-Table Block Delta

Decision: Add an all-table history delta API that merges per-table open-history bundles and exports only sealed blocks missing from the receiver inventory.

Scope impact: compacted-history sync no longer requires callers to manually negotiate every table. This is still table-scope history, not query-scope block planning, but it matches the first peer inventory exchange shape.

## Thu May 28 00:42:26 PDT 2026 - RFC Sync Prototype Alignment

Decision: Update the RFC to treat table/all-table block-native deltas as implemented prototype scope, while keeping query-scoped block planning as future work.

Scope impact: the RFC now matches the current API boundary: receiver manifest inventory, open-history bundle, missing sealed block payloads, and no projection rebuild between block import and hot bundle apply.

## Thu May 28 00:46:24 PDT 2026 - Columnar User Values

Decision: Bump new sealed blocks to v3 `columnar-json-lz4` and store history user values as per-column arrays instead of a per-row map array.

Scope impact: this is still JSON inside lz4, but it removes repeated user column keys from sealed row history and moves the format closer to the RFC columnar target. The spike still keeps v1 bundle-json decode support.

## Thu May 28 01:00:34 PDT 2026 - Local Historical Point Reads

Decision: Add local node-epoch point reads for unglobalized local history and include them in the deep-history benchmark report. The benchmark now samples early, middle, and latest local epochs so sealed blocks are exercised even when transactions have not been assigned global epochs.

Scope impact: this exposed and fixed an accidental slow path where newest-record selection could re-decode sealed blocks per candidate. The remaining Block historical-read numbers still decode and scan an entire selected block per sampled point read, which is the next obvious target for block-local indexing or lighter decoding.

## Thu May 28 01:07:35 PDT 2026 - Decode Cache For Point Reads

Decision: Cache decoded sealed history blocks inside a Runtime and skip sealed blocks whose local-epoch range cannot beat an already-found hot/open candidate.

Scope impact: repeated historical reads against the same cold block no longer repeatedly pay lz4 and JSON decode. This is still an in-memory optimization, not a storage-format answer; early/midpoint reads continue to decode and scan the selected block at least once.

## Thu May 28 01:14:43 PDT 2026 - Measure Sealed Transaction Info

Decision: Add sampled `transaction_info(tx-id)` timing to deep-history benchmarks and route sealed transaction lookup through the Runtime block cache.

Scope impact: the first append measurement showed sealed tx metadata lookup averaging roughly 72 ms through the uncached free-function path. With the cache-aware path, the same sampled lookup is sub-millisecond after the row historical reads have warmed the block. This validates the side-index plus cached decode approach for tx metadata while still leaving cold first-read latency unsolved.

## Thu May 28 01:18:13 PDT 2026 - Query Equality Block Delta

Decision: Add a narrow equality-query history delta API that returns hot/open query history as an ordinary bundle plus missing sealed blocks for the query row set.

Scope impact: query-scoped sync no longer has to be all-or-nothing table delta in the prototype. The first shape covers equality predicates only and still uses the existing repair-row and policy-dependency machinery for open history; sealed history is transferred as blocks instead of expanded into row-history records.

## Thu May 28 01:21:43 PDT 2026 - Delta Apply Helper

Decision: Add `apply_history_delta(bundle, blocks)` as the receiver-side API for block-native deltas.

Scope impact: table, all-table, and equality-query deltas now share one receiver call shape: import any missing sealed blocks, then apply the hot/open bundle. This reduces call-site footguns while keeping the operation semantics explicit.

## Thu May 28 01:23:54 PDT 2026 - Simple Predicate Block Deltas

Decision: Extend query-scoped history deltas from equality to `contains`, `in`, and `ne` predicates using the same open-bundle plus missing-block transfer shape.

Scope impact: the first query-scoped block-native sync family now covers the simple predicate operators used by ordinary observed query refreshes. Top-N/page operators still need their own planning because they carry boundary/previous-observed state.

## Thu May 28 01:27:52 PDT 2026 - Typed History Delta

Decision: Introduce a `HistoryDelta` return type for block-native sync APIs instead of anonymous `(Bundle, Vec<HistoryBlockExport>)` tuples.

Scope impact: table, all-table, and query predicate deltas now expose the same named shape. This keeps receiver code aligned with `apply_history_delta` and makes the API harder to misuse as more query operators gain block-native variants.

## Thu May 28 01:30:06 PDT 2026 - Top Query Block Deltas

Decision: Add block-native history delta APIs for top-created and top-field query shapes without previous-observed repair state.

Scope impact: page-like query sync can now transfer matching sealed row blocks without expanding them into ordinary history when bootstrapping a top query. Previous-observed refresh variants still need a follow-up API because their repair row set is caller-provided state.

## Thu May 28 01:33:50 PDT 2026 - JSON XY String Column Codec

Decision: Bump newly sealed blocks to format v4 and add a column codec for text values that are JSON strings shaped like `{x, y}` numeric objects.

Scope impact: canvas-style coordinate columns now store sealed values as numeric `x[]`/`y[]` arrays inside the columnar block and decode back to the same text value. This is a deliberately narrow bridge toward per-column codecs; the canonical canvas block payload fell from roughly 206 KB to 200 KB, so the idea helps but is not enough by itself.

## Thu May 28 01:38:39 PDT 2026 - Top Created Repair Delta

Decision: Add a previous-observed variant for top-created block-native history deltas.

Scope impact: top-created observed refreshes can now include caller-provided previous row ids in the open-history repair set while still sending matching sealed history as blocks. I deferred the equivalent top-field previous-observed method because its signature needs a small options type rather than another long positional API.
