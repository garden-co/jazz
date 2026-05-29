# Deep History Implementation Decision Log

Timebox start: Wed May 27 22:52:41 PDT 2026
Timebox target end: Thu May 28 04:52:41 PDT 2026

## Thu May 28 04:12:19 PDT 2026

Decision: rerun Block benchmarks after the tx-reference validation landed.

Why: even small validation additions affect the block-native import measurement, and the comparison table should describe the code at HEAD. The extra tx-id checks did not materially change the shape: import remains around 273-303 ms for append/Automerge and 296 ms for canvas in this run.

Scope impact: `/tmp/deep_history_all_canonical_blocks_final.json` is the latest canonical Block run, and the Block column was refreshed from that output.

## Thu May 28 04:09:29 PDT 2026

Decision: reject sealed block payloads whose history/read records reference missing tx records.

Why: block-local history and read streams rely on the tx section for public tx identity, epochs, outcomes, and metadata. Accepting a block with dangling tx references would defer corruption until rebuild, sync export, or transaction lookup.

Scope impact: import validation now requires every history `tx_id` and read `tx_id` in a block payload to appear in that block's tx section. Unit tests cover both dangling history and dangling read records.

## Thu May 28 04:07:17 PDT 2026

Decision: document the accepted-block settledness gap instead of hiding it.

Why: the current prototype compacts visible non-rejected local history, and import validation therefore accepts non-rejected txs in accepted blocks while rejecting rejected txs. The RFC's ideal says accepted blocks should contain genuinely settled accepted txs only, so the prototype status needs to call out that remaining semantic tightening explicitly.

Scope impact: the RFC partial-status list now records that production should tighten accepted-block compaction once the runtime has a crisp accepted-vs-optimistic cutoff.

## Thu May 28 04:06:08 PDT 2026

Decision: refresh the Block benchmark column after import validation.

Why: block-native import now decodes compressed payloads to validate manifest and tx-index summaries before insertion. That is a real semantic cost and should be visible in the canonical comparison table.

Scope impact: reran `MINI_JAZZ_PERF_ONLY_DEEP_HISTORY=all-history-blocks` with canonical inputs. The JSON output is saved at `/tmp/deep_history_all_canonical_blocks_post_validation.json`, and the Block column now reflects the slower validated import path.

## Thu May 28 04:03:26 PDT 2026

Decision: validate imported `uncompressed_bytes` without double-decompressing blocks.

Why: compressed byte count and payload hash are not enough to make manifest accounting trustworthy. `uncompressed_bytes` is used for stats and benchmark interpretation, so it should reflect the actual decoded payload size.

Scope impact: history block decoding is split into "decompress bytes" and "parse bytes" helpers. Import validation checks `uncompressed_bytes` against the decompressed payload before parsing the bundle.

## Thu May 28 04:01:51 PDT 2026

Decision: keep the RFC prototype-status checklist synchronized with the overnight implementation.

Why: the decision log captures chronological detail, but the RFC should remain the compact agreement surface. Import-boundary validation and grouped benchmark selectors are now real enough to be listed as implemented.

Scope impact: the RFC status section now names block import validation and grouped deep-history benchmark selectors explicitly.

## Thu May 28 04:01:03 PDT 2026

Decision: reject duplicate imported tx-range entries for the same node.

Why: `history_block_tx_index` is a summary index, not a row-by-row block payload. Each block should have one canonical local-epoch range per node; accepting duplicates would make sync payloads non-canonical and could hide malformed ranges behind map deduplication during validation.

Scope impact: raw block import now fails duplicate range entries before comparing against the decoded tx summary.

## Thu May 28 03:59:58 PDT 2026

Decision: require imported tx-range indexes to exactly match decoded block txs.

Why: the tx-range side index is what lets `transaction_info(tx-id)` and point reads find sealed block payloads without scanning every block. If a peer can attach a valid payload to a wider or wrong node/local range, later reads may decode the wrong block set or miss the right one.

Scope impact: import derives the expected `(node_id, min_local_epoch, max_local_epoch)` ranges from decoded `bundle.txs` and rejects mismatches. The raw-block sync test now covers both impossible ranges and plausible-but-wrong ranges.

## Thu May 28 03:58:57 PDT 2026

Decision: validate imported block manifests against decoded payload summaries.

Why: `payload_sha256` covers the compressed bytes, not the surrounding manifest. A peer should not be able to import a block whose payload is valid but whose row count, tx count, row identity, kind/outcome family, or epoch envelope lies to the SQLite indexes.

Scope impact: raw block import now decodes the payload during validation and rejects manifest/payload mismatches before inserting `history_blocks` or `history_block_tx_index` rows. The raw-block sync test covers a row-count mismatch as the representative failure.

## Thu May 28 03:56:06 PDT 2026

Decision: reject imported raw history blocks with nonsensical tx ranges.

Why: raw block sync already treats the payload hash and compressed size as integrity boundaries. The tx index is equally important for later tx lookup and delta selection, so malformed ranges should fail at import instead of creating impossible index rows.

Scope impact: import now rejects any `HistoryBlockTxRange` whose `min_local_epoch` is greater than `max_local_epoch`. The raw-block sync test covers this alongside hash validation.

## Thu May 28 03:52:33 PDT 2026

Decision: verify the grouped Block selector with real canonical caps.

Why: Block is the RFC's main compacted-history path, and the grouped selector should exercise compaction, block-native export/import, historical reads, and tx-info lookup across all three scenarios in one command.

Scope impact: `MINI_JAZZ_PERF_ONLY_DEEP_HISTORY=all-history-blocks` completed append, Automerge, and canvas at canonical update counts. The output was saved in `/tmp/deep_history_all_canonical_blocks.json`; numbers remained in line with the existing Block table, so the benchmark comparison table was left unchanged.

## Thu May 28 03:51:24 PDT 2026

Decision: verify the documented grouped baseline selector with real canonical caps, but do not rewrite the comparison table from that smoke.

Why: the run was intended to validate benchmark ergonomics after adding grouped selectors and per-scenario sample intervals. The current codebase already includes later storage-format changes, so treating this as a fresh historical Base column update would muddy the table.

Scope impact: `MINI_JAZZ_PERF_ONLY_DEEP_HISTORY=all` completed append, Automerge, and canvas at the canonical update counts. The output was saved in `/tmp/deep_history_all_canonical_base.json` for handoff context only.

## Thu May 28 03:49:47 PDT 2026

Decision: keep policy denials per-logical-transaction inside grouped SQLite commits.

Why: batching should change the durable commit boundary, not Jazz policy semantics. A denied write is still a rejected Jazz transaction, while allowed siblings in the same SQLite batch can commit.

Scope impact: batched inserts now have a regression with one allowed comment and one policy-denied comment. The batch returns both tx ids, commits the allowed row, and records `policy_denied` only on the denied tx.

## Thu May 28 03:48:16 PDT 2026

Decision: add grouped deep-history selectors for Block probes.

Why: the comparison table is organized by experiment column, so benchmark smoke runs should be able to run all three scenarios for the baseline or Block shapes without invoking unrelated perf probes.

Scope impact: `MINI_JAZZ_PERF_ONLY_DEEP_HISTORY=all-history-blocks` runs the three Block probes. A tiny smoke run verified the selector.

## Thu May 28 03:47:01 PDT 2026

Decision: add scenario-specific deep-history sample interval env vars.

Why: `MINI_JAZZ_PERF_ONLY_DEEP_HISTORY=all` is most useful when append, Automerge, and canvas can run together while preserving their canonical sample cadence. A single shared `MINI_JAZZ_DEEP_HISTORY_SAMPLE_EVERY` forces an awkward compromise.

Scope impact: append, Automerge, and canvas now accept `MINI_JAZZ_DEEP_HISTORY_APPEND_SAMPLE_EVERY`, `MINI_JAZZ_DEEP_HISTORY_AUTOMERGE_SAMPLE_EVERY`, and `MINI_JAZZ_DEEP_HISTORY_CANVAS_SAMPLE_EVERY`, falling back to the existing shared env var and then each scenario default. A tiny `all` smoke run verified the override path.

## Thu May 28 03:44:57 PDT 2026

Decision: pin batched-write rollback semantics with an integration test.

Why: grouped commits intentionally move multiple logical Jazz transactions under one SQLite transaction. If validation fails partway through, earlier logical writes in that group must not leak into history/current state.

Scope impact: batched inserts now have a regression where the second write has an unknown field; the API returns an error and the first write is rolled back with zero history rows.

## Thu May 28 03:43:48 PDT 2026

Decision: add `MINI_JAZZ_PERF_ONLY_DEEP_HISTORY=all` as a deep-history-only benchmark selector.

Why: leaving the selector unset can run the broader perf suite, while tonight's smoke check needed only the three deep-history scenarios. The missing `all` alias caused a false start and made benchmark iteration easier to misuse.

Scope impact: `all`/`canonical` now run append, Automerge, and canvas deep-history probes as a JSON array. The benchmark doc notes this is best for smoke checks or shared env caps; canonical table updates still use scenario-specific sample intervals.

## Thu May 28 03:42:52 PDT 2026

Decision: smoke-test the deep-history benchmark write-batching path with a tiny append run instead of chasing fresh numbers tonight.

Why: the combined small scenario run produced no early output and was killed to avoid burning the timebox. The important post-refactor check is that `MINI_JAZZ_DEEP_HISTORY_WRITE_BATCH_SIZE` still drives the benchmark path.

Scope impact: a 100-update append smoke run completed with batching enabled and reported the expected note: "SQLite write batching enabled: up to 64 logical Jazz txs per SQLite commit." No canonical benchmark numbers were updated from this smoke run.

## Thu May 28 03:39:06 PDT 2026

Decision: add explicit compacted-history coverage for hyphenated node ids in public tx ids.

Why: production node ids may be UUIDs. Public tx ids are currently derived as `tx-{node_id}-{local_epoch}`, so every parser and sealed tx lookup path must split from the right, not assume simple hyphen-free node ids.

Scope impact: a sealed transaction lookup regression now uses a UUID-shaped node id, compacts the row history, and verifies transaction metadata/write-row lookup still resolves through the block index.

## Thu May 28 03:37:36 PDT 2026

Decision: round out the narrow grouped-commit prototype with explicit batched inserts.

Why: the write-policy stretch goal covers both imports and high-frequency streams. Imports often start with many creates, so update/upsert-only batching leaves a common fast path unrepresented.

Scope impact: the prototype now has explicit insert, update, and upsert batch APIs. They all share one SQLite commit while preserving one Jazz tx per logical row write.

## Thu May 28 03:35:18 PDT 2026

Decision: cover recursive-reference branch history deltas with the same future-block omission invariant.

Why: recursive sync uses a separate block attachment path and can matter for tree-shaped documents or canvases. It must not import accepted main history newer than the branch base just because a descendant row has compacted history.

Scope impact: recursive branch deltas now have a regression that compacts a future main descendant update, exports from the branch, and verifies the receiver sees only the pinned base descendant without receiving the future sealed block.

## Thu May 28 03:33:34 PDT 2026

Decision: widen the batched-write prototype from update-only to batched upserts.

Why: imports and fast streams often include a create phase followed by updates. Requiring callers to split creates from updates makes the benchmark hook less representative of the stretch-goal write policy.

Scope impact: `upsert_rows_batched` chooses create vs update per row inside the shared SQLite transaction while preserving one public Jazz tx id, read set, write set, and history row per logical write.

## Thu May 28 03:30:21 PDT 2026

Decision: cover branch-scoped block filtering through table and all-table history deltas, not only query deltas.

Why: table/all sync paths attach raw sealed blocks through separate code paths. The same atomic-block leak risk applies there when the sender is checked out on a branch with a pinned main base.

Scope impact: regressions now assert that compacted future-main accepted blocks are omitted from branch table/all deltas and that receivers still reconstruct the pinned base row from the preserved open anchor.

## Thu May 28 03:28:36 PDT 2026

Decision: branch-scoped history deltas filter accepted sealed block manifests by branch base epoch before attaching raw blocks.

Why: sealed blocks are atomic. If a branch delta included an accepted block newer than the branch base, the receiver could import future main history that the branch snapshot must not see.

Scope impact: a sealed accepted block is visible to a branch delta only when its maximum global epoch is at or before the branch base. Newer or overlapping blocks stay out; preserved branch-base anchors cover the compacted local branch cases we currently support.

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

## Thu May 28 01:42:41 PDT 2026 - Observed Refresh History Deltas

Decision: Add observed-query refresh APIs that return `HistoryDelta` values instead of only ordinary bundles.

Why: once peers remember observed queries, refresh is the normal sync path. It should be able to transfer missing sealed blocks directly for predicate and top-query reads, including previous-observed repair rows, rather than forcing all sealed history back through row-bundle inflation.

Scope impact: simple predicates, top-created, and top-field observed refreshes now use block-native deltas. Query shapes without block-native support still fall back to ordinary bundle refreshes with no blocks, preserving behavior while making remaining gaps explicit.

## Thu May 28 01:46:19 PDT 2026 - Bounded Compaction Policy

Decision: Add a `HistoryCompactionPolicy` API with accepted/rejected toggles, hot-tail/min-version thresholds, and an optional maximum block budget.

Why: production compaction should be triggerable as bounded maintenance work, not only as exact row-by-row calls. A block budget lets callers do useful background progress without accidentally spending an unbounded foreground latency slice.

Scope impact: the prototype can now compact eligible rows across all user tables in chunks. Scheduling by age, byte estimate, or wall-clock budget remains future work.

## Thu May 28 01:49:22 PDT 2026 - Recursive Ref History Deltas

Decision: Add a block-native history delta for recursive reference queries and route observed recursive refreshes through it.

Why: recursive refs are a common relationship query and their result is still a concrete row set. They should not be forced through ordinary bundle inflation just because the read shape is tree-like.

Scope impact: recursive observed refreshes now send missing sealed blocks for visible recursive rows. Deeper block-native repair for descendants that exist only in sealed deleted history remains an optimization gap.

## Thu May 28 01:53:56 PDT 2026 - String Dictionary Block Columns

Decision: Bump newly sealed columnar blocks to v5 and dictionary-code repeated string columns.

Why: table names, row ids, branch ids, node ids, and user ids are highly repetitive inside per-row history blocks. LZ4 can find some of this, but explicit string dictionaries reduce the JSON payload before compression and keep decoding straightforward.

Scope impact: the decoder accepts v3/v4 columnar blocks and v1 legacy bundle blocks. New v5 blocks keep the v4 JSON `{x,y}` value codec and add dictionary/raw dual decoding for string columns.

## Thu May 28 02:00:12 PDT 2026 - Top Field Repair Delta Options

Decision: Add a public options-object API for top-field block-native history deltas with previous-observed repair rows.

Why: top-field refresh has the same displaced-boundary-row problem as top-created refresh, but adding more positional arguments would make the API brittle. An options object gives us room for future page-boundary or cursor state.

Scope impact: manual top-field history delta callers can now request repair history and missing sealed blocks for rows that were observed in the previous page. Observed-query refresh was already using equivalent internal behavior.

## Thu May 28 02:03:39 PDT 2026 - Wall Clock Compaction Budget

Decision: Extend `HistoryCompactionPolicy` with an optional wall-clock budget.

Why: block-count budgets bound work coarsely, but foreground callers often need a latency-shaped budget. A zero-duration budget should be a valid way to ask the maintenance path to skip work when the caller's slice is already spent.

Scope impact: policy compaction checks the wall-clock budget before starting each row block. Age and byte-estimate triggers remain higher-level scheduling work.

## Thu May 28 02:05:18 PDT 2026 - Storage Stats Block Bytes

Decision: Add sealed history block compressed and uncompressed byte totals to `StorageStats`.

Why: once history can live in both ordinary rows and sealed blobs, row counts and page counts are not enough to explain storage behavior. The block byte totals make it clear how much payload is useful sealed history versus SQLite pages, freelist, and other metadata.

Scope impact: storage stats now expose block payload totals directly. This is observability only; it does not change compaction or sync semantics.

## Thu May 28 02:07:21 PDT 2026 - Serializable History Block Exports

Decision: Make `HistoryBlockExport` serialize and deserialize with its payload bytes.

Why: raw block sync is only real if the transfer artifact includes the compressed payload, not just the manifest and tx ranges. The previous in-memory API could move payloads, but serde output skipped them.

Scope impact: block exports now round-trip through serde JSON in tests. Payload bytes serialize as a hex string, which is not the final compact wire encoding but avoids accidentally shipping manifest-only blocks or huge JSON byte arrays.

## Thu May 28 02:10:50 PDT 2026 - Refresh Delta Block Deduplication

Decision: Deduplicate sealed block exports across a batch of observed-query history deltas.

Why: one peer can remember multiple observed queries that all touch the same sealed row. Sending the same compressed block once per query would waste storage and wire bandwidth precisely where blocks are meant to help.

Scope impact: `export_query_read_refresh_deltas` now treats remote manifests plus blocks already emitted earlier in the same call as known. Each returned delta remains individually applyable in order, but shared block payloads appear only once in the batch.

## Thu May 28 02:13:16 PDT 2026 - Compressed Byte Compaction Budget

Decision: Extend `HistoryCompactionPolicy` with an optional compressed-payload byte budget.

Why: block-count and wall-clock budgets are useful but do not directly bound how much sealed payload maintenance emits. A compressed-byte budget lets callers make bounded progress in storage terms and complements the block-count limit.

Scope impact: policy compaction checks compressed bytes before starting each next row block. The budget is post-block rather than a preflight estimate, so it can overshoot by one block; richer byte estimation remains future scheduling work.

## Thu May 28 02:15:59 PDT 2026 - Rows Per History Block

Decision: Add a `max_rows_per_block` compaction policy knob for accepted history.

Why: one huge block per deep row is good for compression but bad for cold point reads, because the first read has to decode and scan the whole blob. Smaller per-row blocks give us a direct storage/read-latency tradeoff without changing semantics.

Scope impact: policy compaction can now split one row's compacted accepted history into multiple blocks. Existing direct row/table compaction keeps the old single-block behavior unless the policy knob is used.

## Thu May 28 02:20:35 PDT 2026 - Rejected Block Splitting

Decision: Apply the `max_rows_per_block` policy knob to rejected-history compaction too.

Why: rejected histories can also grow deep, especially when a client repeatedly retries an invalid write. They should get the same block-size tradeoff as accepted histories and remain separate from accepted blocks.

Scope impact: policy compaction now splits accepted and rejected row histories. Direct rejected row/table compaction keeps the existing single-block behavior unless called through the policy path.

## Thu May 28 02:22:50 PDT 2026 - Benchmark Block Size Knob

Decision: expose `MINI_JAZZ_DEEP_HISTORY_MAX_ROWS_PER_BLOCK` in the deep-history block benchmarks.

Why: one huge block is the best-case compression point but the worst-case first cold point-read unit. We need to be able to measure smaller block sizes against the same canonical write workloads before choosing a production default or adaptive policy.

Scope impact: the default Block column remains one block per compacted row history. Setting the env var routes benchmark compaction through `HistoryCompactionPolicy` and records the row cap in the benchmark notes.

## Thu May 28 02:26:31 PDT 2026 - User Value Dictionaries

Decision: bump newly sealed columnar blocks to v6 and dictionary-code repeated user column values.

Why: the whole-row thesis depends on compressing unchanged columns, not just large edited text blobs or repeated Jazz metadata. In a wide row, many columns can remain byte-identical across a deep edit history, and those should become one dictionary value plus small refs inside the sealed block.

Scope impact: non-coordinate user value columns now choose a JSON-value dictionary when values repeat. The decoder remains compatible with v3 through v5 columnar blocks and v1 bundle-json blocks.

## Thu May 28 02:29:19 PDT 2026 - Integer Run/Delta Columns

Decision: bump newly sealed columnar blocks to v7 and encode integer metadata columns as runs or deltas.

Why: local epochs, transaction outcomes, read reasons, history op codes, and timestamps are often monotonic or constant over a sealed block. Raw JSON integer arrays preserve semantics but waste bytes before lz4 even sees the payload.

Scope impact: tx/read/history integer columns now choose run, delta, or raw encoding per column. Older v3 through v6 columnar blocks still decode because raw arrays remain accepted.

## Thu May 28 02:33:19 PDT 2026 - Nullable Epoch Runs

Decision: bump newly sealed columnar blocks to v8 and run-code nullable integer metadata, starting with transaction global epochs.

Why: local-only history commonly has no global epochs yet, so the block payload otherwise carries long raw arrays of `null`. That is semantically simple but unnecessary overhead in exactly the deep local-write workloads we are studying.

Scope impact: `global_epoch` now chooses a nullable run column when repeated null or repeated epoch values dominate. Older v3 through v7 columnar blocks still decode.

## Thu May 28 02:38:30 PDT 2026 - Repeated Tx Detail Runs

Decision: bump newly sealed columnar blocks to v9 and run-code repeated nullable JSON metadata plus repeated integer-vector metadata.

Why: accepted blocks usually have no rejection details, and receipt tiers often repeat. Leaving those as raw per-transaction JSON arrays preserves lots of `null` and `[]` scaffolding even after earlier string and integer column compression.

Scope impact: transaction rejection details and receipt tiers now choose run columns when repeated. Older v3 through v8 columnar blocks still decode.

## Thu May 28 02:44:35 PDT 2026 - Block Size Tradeoff Looks Real

Decision: keep `max_rows_per_block` as a first-class compaction policy knob rather than treating one block per deep row as the obvious default.

Why: a quick append sweep showed historical point-read average improving as the per-block row cap dropped, while compressed payload bytes stayed in the same order of magnitude. The exact optimum will vary by workload, but the policy needs to expose this storage/read-latency tradeoff.

Scope impact: no API change in this step; document the measured non-canonical sweep so future tuning can compare against it.

## Thu May 28 02:46:28 PDT 2026 - Point Reads Choose One Candidate Block

Decision: limit node-local sealed point reads to the newest candidate block range instead of decoding every older block up to the requested epoch.

Why: after block splitting, the old lookup defeated the point-read benefit by returning all sealed blocks whose max epoch was newer than the open-history candidate. A point lookup only needs the newest sealed block that can contain or precede the requested local epoch.

Scope impact: split-block historical reads decode fewer blocks. The append cap-100 probe improved from roughly 57 ms to roughly 43 ms average historical read time with the same logical results. Global-epoch point reads use the same one-candidate-block limit for non-overlapping block ranges.

## Thu May 28 02:49:19 PDT 2026 - Sealed Global Reads Are As-Of Reads

Decision: make sealed global-epoch point reads choose the newest sealed block at or before the requested epoch, not only blocks whose range contains the exact epoch.

Why: open-history point reads are "as of" reads. If compaction seals the visible head and a caller asks for a later global epoch, the sealed path should still return the latest prior row version instead of pretending no history exists.

Scope impact: `read_row_at_global_epoch` now works after `hot_tail = 0` compaction even when the requested epoch is later than the last sealed transaction.

## Thu May 28 02:51:31 PDT 2026 - Cache Global Point Read Blocks

Decision: route global sealed point reads through the runtime history block cache.

Why: node-local point reads and transaction lookup already avoid repeated lz4/JSON decode inside one `Runtime`. Global point reads should share that behavior instead of using an uncached free function path.

Scope impact: repeated global historical reads against the same sealed block can reuse the decoded block in memory. The logical API is unchanged.

## Thu May 28 02:54:53 PDT 2026 - Codec Choices Must Earn Their Bytes

Decision: choose run, delta, and dictionary value columns only when their serialized JSON representation is smaller than the raw column.

Why: with JSON as the temporary block container, a clever physical column can be worse for shallow or high-entropy columns because the variant object names cost bytes. The encoder should make this decision per column rather than assuming a codec is always better.

Scope impact: block format stays v9; only the encoder's choice policy changes. Canonical Block payloads improved in the measured append, Automerge, and canvas runs.

## Thu May 28 02:57:22 PDT 2026 - Bound The Decoded Block Cache

Decision: cap the per-runtime decoded history block cache at 64 blocks.

Why: the cache improves repeated historical reads and transaction lookups, but an unbounded cache undermines the memory part of the RFC goal. Decoded blocks should be temporary accelerators, not a way for point reads to pin all cold history in RAM.

Scope impact: all runtime block-cache users now share one bounded insertion path. The eviction policy is intentionally simple for the spike; a later version can make it true LRU if measured.

## Thu May 28 03:02:14 PDT 2026 - Full Suite Checkpoint After Cache Bound

Decision: treat the v9 columnar block encoder, one-candidate point reads, runtime block cache, and 64-block cache bound as a stable checkpoint before starting another feature slice.

Why: the full `cargo test -p mini-jazz-sqlite` suite passes with 12 unit tests and 377 whole-system tests. That gives us a clean base for the remaining timebox instead of stacking new work on top of an unverified cache change.

Scope impact: keep subsequent edits small and RFC-shaped unless a measured benchmark gap justifies a larger change.

## Thu May 28 03:03:16 PDT 2026 - Make The Block Cache LRU

Decision: change the bounded decoded history block cache from "evict the smallest local block id" to least-recently-used eviction.

Why: historical reads often touch nearby points repeatedly before moving elsewhere. Local block ids are not a good proxy for usefulness, especially after sync import or block splitting. LRU keeps the recently touched decompressed blocks while preserving the fixed memory cap.

Scope impact: add a small order queue beside the cache map and focused tests for both the bound and eviction policy. The cache remains an implementation detail of `Runtime`.

## Thu May 28 03:05:23 PDT 2026 - Use The Cache For Export Block Decodes

Decision: route table-history export and query-scope sealed-history stitching through the runtime decoded block cache.

Why: repeated exports, observed refreshes, and query-scope repairs can touch the same sealed blocks repeatedly. If point reads and transaction lookups benefit from cached lz4/JSON decode, export paths should share that behavior too rather than quietly paying the cold decode cost on every call.

Scope impact: move the accepted-block decode helpers onto `Runtime` so they can call `cached_history_block`. The exported logical bundle shape is unchanged.

## Thu May 28 03:06:53 PDT 2026 - Align The RFC With One Physical Block Table

Decision: update the RFC to describe accepted and rejected history as separate `block_kind` families inside one `history_blocks` table, rather than separate physical tables.

Why: the implementation already proved that the payload codec, tx-range indexes, payload hashes, byte accounting, import path, and manifest comparison are shared. Keeping one table with kind filters gives the same logical separation with less schema and sync surface area.

Scope impact: accepted blocks remain the only blocks used for visible history and accepted point reads. Rejected blocks stay explicit diagnostic/replay data even though they share storage machinery.

## Thu May 28 03:09:54 PDT 2026 - Branch Exports Filter Sealed Main History By Base Epoch

Decision: when exporting table history from a pinned branch, filter sealed main-branch records to the branch base epoch before merging them into the bundle.

Why: ordinary open-history branch export already avoids leaking main history that happened after the branch base. Once main history is sealed, blindly appending the whole accepted block would reintroduce those future main versions and violate branch snapshot semantics.

Scope impact: branch-local sealed records are still retained, while main sealed records whose tx global epoch is newer than the branch base are omitted along with their sealed read/tx records. Added a regression test that failed before the filter.

## Thu May 28 03:14:38 PDT 2026 - Query Exports Can Discover Sealed Branch Base Rows

Decision: branch query-scope exports should scan sealed accepted blocks for rows whose latest main value at the branch base matches the query predicate.

Why: once `hot_tail = 0` can seal the branch base, the current/open query path may not discover a matching base row at all. Exporting only the rows returned by current query evaluation would omit the pinned base row, while exporting all sealed history would leak future main versions. The query exporter therefore needs a narrow sealed-base predicate repair path.

Scope impact: support equality, not-equal, contains, and in predicates over sealed branch-base records. The table and query sealed-branch regression tests now cover both over-export and under-export.

## Thu May 28 03:18:08 PDT 2026 - Refresh Canonical Block Benchmarks

Decision: refresh the benchmark table's Block column after the sealed-read/cache/export fixes.

Why: the storage numbers stayed in the same range, but historical point-read averages moved materially: append is now about 41 ms, Automerge about 61 ms, and canvas about 99 ms in the canonical one-block runs. The table should reflect the current code before the next optimization cycle.

Scope impact: update only the canonical Block column cells from fresh append, Automerge, and canvas runs. The Base/Base1/Base2/Base3/Incr columns are unchanged.

## Thu May 28 03:18:55 PDT 2026 - Sealed Predicate Repair Ignores Unsupported Operators

Decision: make the sealed branch-base predicate repair return no extra rows for unsupported operators instead of failing the export.

Why: the repair path is an optimization/correctness bridge for ordinary predicate scopes. Top, recursive, or future operators should not fail merely because this sealed-base scan does not know how to evaluate them yet; they can continue through their existing open-history/export paths.

Scope impact: equality, not-equal, contains, and in remain supported. Other operators simply get no sealed-base predicate repair from this helper.

## Thu May 28 03:20:22 PDT 2026 - Preserve Branch Base Anchors In Open History

Decision: accepted compaction should not seal the latest main row version at any live branch base epoch.

Why: ordinary branch reads use the current/open sparse-overlay model and should stay fast without decoding cold blocks. If compaction with `hot_tail = 0` removes the exact main row version that a branch is pinned to, local branch reads can no longer materialize the base row. Keeping one sparse anchor per pinned base epoch is a small storage cost for preserving branch snapshot semantics and read performance.

Scope impact: compaction still seals older and newer accepted history, but skips branch-base anchor txs. Added a regression test for local branch reads after history compaction.

## Thu May 28 03:21:15 PDT 2026 - Document Branch Base Anchor Policy

Decision: update the RFC reads section to describe branch-base anchors as the current prototype policy.

Why: the earlier RFC left branch snapshots open-ended between sealed history and future sparse caches. The implemented policy is more concrete: keep sparse base anchors in `history_open` so ordinary branch reads remain cheap, while using blocks for colder non-anchor history and block-native sync/rebuild.

Scope impact: documentation only.

## Thu May 28 03:23:34 PDT 2026 - Tie-Break Sealed Branch Predicate Repair

Decision: when scanning sealed branch-base rows for query repair, choose the latest record by `(global_epoch, local_epoch)` rather than only `global_epoch`.

Why: branch base snapshots already care about deterministic latest-row choice when multiple transactions share a global epoch. The sealed predicate repair does not have local physical `tx_num` from the sender, but including local epoch is a better stable ordering than epoch alone.

Scope impact: no API change. Focused branch sealed-history tests and clippy pass.

## Thu May 28 03:24:19 PDT 2026 - Add Prototype Status To RFC

Decision: add an explicit implemented/partial status section to the sealed-history RFC.

Why: the overnight spike now covers more than the original design sketch: compaction, block-native deltas, columnar codecs, cache policy, branch anchors, and benchmark integration are all real. The RFC should make the boundary visible so the next iteration can focus on the remaining hard problems instead of rediscovering what is already in place.

Scope impact: documentation only.

## Thu May 28 03:25:12 PDT 2026 - Cover Branch Query History Delta

Decision: add a regression test for branch query history deltas after accepted-history compaction.

Why: bundle query export and block-native query delta export are separate surfaces. Both must respect pinned branch bases and avoid leaking future main history once compaction has moved cold history around.

Scope impact: test-only coverage for `export_query_where_eq_history_delta` plus `apply_history_delta` in a compacted branch-base scenario.

## Thu May 28 21:17:25 PDT 2026 - Cache Exact Applied Transactions During Receive

Decision: add a runtime-local exact applied-tx cache for `apply_bundle`.

Why: the canonical live receive benchmark currently applies cumulative table-history bundles. After `Block+Ops3`, live apply dominated total loop time. Exact txs that this runtime has already fully applied can skip repeated tx upsert, read tuple append, and history/current work on later overlapping bundles. The cache is intentionally conservative: it only marks a tx after a bundle carried history for that tx, and it only reuses the cache when outcome, conflict mode, global epoch, and awaiting-dependency state still match. This preserves fate-before-history and policy-dependency cases, which the whole-system tests caught during the first too-eager attempt.

Scope impact: canonical `all-block-ops` receive/update moved from roughly `0.289 -> 0.201` ms for append, `0.281 -> 0.196` ms for Automerge, and `0.223 -> 0.136` ms for canvas. `cargo test -q -p mini-jazz-sqlite` passes.

## Thu May 28 21:24:19 PDT 2026 - Prototype Incremental Open-History Live Export

Decision: add an experimental single-writer open-history export path, gated in the deep-history benchmark by `MINI_JAZZ_DEEP_HISTORY_INCREMENTAL_LIVE_FILTER=1`.

Why: after exact receive idempotency caching, live export and encode/decode became comparable to live apply. Filtering after full export proved smaller bundles help, but export still paid the full scan cost. The new path exports visible open history for one writer node after a known local epoch directly in SQL, then includes the corresponding reads, txs, and branch metadata. This is intentionally narrower than a complete sync protocol: it does not yet cover deleted rows, sealed blocks, multi-node watermarks, or policy dependency expansion.

Scope impact: with the env flag enabled on canonical `all-block-ops`, receive/update moved from roughly `0.201 -> 0.135` ms for append, `0.196 -> 0.130` ms for Automerge, and `0.136 -> 0.103` ms for canvas. Default behavior is unchanged. `cargo test -q -p mini-jazz-sqlite` passes.

## Thu May 28 21:33:13 PDT 2026 - Cache Node Numbers During Bundle Apply

Decision: cache `node_id -> node_num` lookups within one `apply_bundle` call.

Why: the incremental live benchmark applies many transactions from the same node in each receive batch. Even with `INSERT ... RETURNING` for tx upserts, repeatedly ensuring the same node still showed up inside `txs_ms`. A per-call cache keeps the durable semantics identical while avoiding redundant node table work.

Scope impact: with the incremental live-export env flag enabled, live apply/update moved from roughly `0.0446 -> 0.0380` ms for append, `0.0445 -> 0.0372` ms for Automerge, and `0.0433 -> 0.0366` ms for canvas. The gain is almost entirely `txs_ms`, which drops from about `0.017` to `0.010` ms/update. `cargo test -q -p mini-jazz-sqlite` passes.

## Thu May 28 21:35:42 PDT 2026 - Set Received Read/Write Tuples Together

Decision: when receiving a bundle, collect read tuples and write tuples first, then update each affected `jazz_tx` once with both tuple columns.

Why: the previous receive path appended reads before applying history and appended writes after applying history. For the common one-row transaction, that made two `jazz_tx` updates per received tx. The new path keeps the same row-per-update history model and JSONB tuple columns, but writes the received tuple state in one durable update.

Scope impact: with the incremental live-export env flag enabled, live apply/update moved from roughly `0.0380 -> 0.0331` ms for append, `0.0372 -> 0.0331` ms for Automerge, and `0.0366 -> 0.0325` ms for canvas. `reads_ms` drops from about `0.0085` to `0.0021` ms/update; `history_ms` rises slightly because it now owns the combined tuple write. `cargo test -q -p mini-jazz-sqlite` passes.

## Thu May 28 21:40:38 PDT 2026 - Insert Local Tx Tuples Directly

Decision: for batched local single-row writes, defer creating the `jazz_tx` row until the row number and read tuple are known, then insert the tx with `writes_json` and `reads_json` already populated.

Why: the hot local write path was creating a tx row and then immediately updating that same row to fill the compact read/write tuple columns. That made tuple maintenance a full SQLite update per application update. The deferred insert keeps the same one-jazz-history-row-per-update semantics, but turns tx tuple maintenance into part of the tx insert itself. While here, field validation stopped rebuilding a schema-field set for every write.

Scope impact: with the incremental live-export env flag enabled, write-only/update moved from roughly `0.0559 -> 0.0496` ms for append, `0.0855 -> 0.0824` ms for Automerge, and `0.0302 -> 0.0249` ms for canvas. `tx_tuple_ms` drops to near zero; `tx_create_ms` rises slightly because it now includes tuple payload insertion. `cargo test -q -p mini-jazz-sqlite` passes.

## Thu May 28 21:44:01 PDT 2026 - Export Incremental Txs By Node Range

Decision: teach the experimental incremental live export path to export tx records by `(node_num, local_epoch >= watermark)` instead of building a string `IN (...)` over public tx ids.

Why: in the realtime single-writer path, all newly exported history belongs to one writer node and a contiguous local-epoch range. Going through `jazz_tx_public` reconstructs tx ids in SQL and forces string matching. The range exporter resolves `node_num` once, builds tx ids in Rust, and falls back to the generic tx-id exporter only for missing cross-node dependencies.

Scope impact: live export/update with the incremental env flag moved from roughly `0.0461 -> 0.0395` ms for append, `0.0457 -> 0.0396` ms for Automerge, and `0.0452 -> 0.0392` ms for canvas in the sample run. `cargo test -q -p mini-jazz-sqlite` passes.

## Thu May 28 21:51:48 PDT 2026 - Batch Tx Upserts During Bundle Apply

Decision: apply bundle tx records with multi-row `INSERT ... ON CONFLICT ... RETURNING` chunks instead of one upsert statement per tx.

Why: after direct tuple writes and incremental export, receive still paid one SQLite tx-row upsert for every incoming Jazz transaction. Live bundles in the benchmark contain hundreds of txs from the same ingest slice, so the receive path should use that batch shape while preserving the same durable `jazz_tx` rows and cache eligibility checks.

Scope impact: with the incremental live-export env flag enabled, apply `txs_ms/update` moved from roughly `0.0102 -> 0.0088` ms for append, `0.0102 -> 0.0086` ms for Automerge, and `0.0104 -> 0.0087` ms for canvas. Total apply/update moved from roughly `0.0332 -> 0.0324`, `0.0332 -> 0.0316`, and `0.0326 -> 0.0308` ms respectively. `cargo test -q -p mini-jazz-sqlite` passes.

## Thu May 28 21:54:06 PDT 2026 - Batch Received History Inserts

Decision: during bundle apply, collect received history rows per table and insert them with multi-row `INSERT OR REPLACE` batches before repairing current projection.

Why: the receive path still inserted one open-history row at a time. Since bundle apply already owns a SQLite transaction and current projection repair only needs history to be durable before repair, we can batch the history table writes without changing the one-history-row-per-update storage model.

Scope impact: with the incremental live-export env flag enabled, apply `history_ms/update` moved from roughly `0.0193 -> 0.0164` ms for append, `0.0188 -> 0.0157` ms for Automerge, and `0.0182 -> 0.0147` ms for canvas. Total apply/update moved to roughly `0.0292`, `0.0284`, and `0.0275` ms respectively. `cargo test -q -p mini-jazz-sqlite` passes.

## Thu May 28 21:56:00 PDT 2026 - Batch Received Tuple Updates

Decision: update received `jazz_tx.writes_json` and `reads_json` with one CTE `VALUES` batch instead of one `UPDATE` per tx.

Why: after batching history inserts, the remaining `history_ms` bucket still included a per-tx tuple-column update. The data is already grouped by receive bundle, so a CTE batch keeps the same JSONB tuple columns while reducing SQLite statement count.

Scope impact: with the incremental live-export env flag enabled, apply `history_ms/update` moved from roughly `0.0164 -> 0.0133` ms for append, `0.0157 -> 0.0124` ms for Automerge, and `0.0147 -> 0.0120` ms for canvas. Total apply/update moved to roughly `0.0264`, `0.0251`, and `0.0245` ms respectively. `cargo test -q -p mini-jazz-sqlite` passes.
