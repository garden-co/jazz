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
