# jazz-lsm single-threaded performance plan (WASM/OPFS first)

Improve `jazz-lsm` mixed read/write performance in a single-threaded runtime, targeting "good enough in browser" while keeping native usable.

## Constraints

- Single dedicated worker in WASM (`FileSystemSyncAccessHandle` owner)
- Synchronous DB API remains unchanged
- No background threads required for correctness or performance
- Maintenance work must be cooperative (bounded foreground steps)
- Durability contract unchanged: no lost acknowledged writes after `flush_wal()`

## Roadmap status (updated 2026-02-14)

| Phase | Item | Status | Complexity | Expected mixed R/W impact | Notes |
|---|---|---|---|---|---|
| 1 | Mixed workload benchmarks + counters | Done | S | Baseline quality | Implemented for native + WASM |
| 2 | Internal write batching + in-memory WAL byte tracking | Done | S-M | High | Highest payoff-per-complexity so far |
| 3 | Reuse encode/decode buffers in write path | Done | S | Medium | Positive native, mixed WASM |
| 4 | SST v2 block format + point-read block index | Done | M-H | High but variable | Keep selective parts in Phase 8 stack |
| 5 | Per-SST bloom filters | Done | M | Low/negative net | Kept for now; may revisit/trim later |
| 6 | SST metadata/index cache + small block cache | Done | M | High | Highest payoff-per-complexity so far |
| 7 | Range-scoped compaction + per-step budget | Dropped | M-H | Negative net in current runs | Dropped after strong native regressions |
| 8 | `phase_2_6_some_4` consolidation | Done (mixed) | S-M | Mixed; not clearly better than Phase 6 | Finalized at `32 KiB` blocks + bloom enabled |
| 9 | Append-only manifest edits + periodic checkpoint | Done (mixed) | M | High | Large gains vs Phase 8, especially native write-heavy rows |
| 10 | Large-value separation (blob log threshold) | Planned | H | +5x to +20x for 1MB-heavy workloads | Still likely required for sustained 1MB performance |

## Measured ROI update (2026-02-14)

Observed payoff-per-complexity from completed phases:

1. `Phase 2` (internal WAL batching + in-memory WAL bytes): highest ROI, low complexity.
2. `Phase 9` (append-only manifest edits + periodic checkpoint): highest ROI, medium complexity.
3. `Phase 6` (SST metadata/index cache + block cache): highest ROI, medium complexity.
4. `Phase 3` (buffer reuse): good native ROI, mixed wasm impact.
5. `Phase 4` (SST v2 block format + point index): high upside but more volatility/complexity.
6. `Phase 5` (bloom filters): low/negative net in current runs.
7. `Phase 7` (range-scoped compaction + per-step budget): negative net for native in current runs.

Decision for next iteration: build and benchmark **Phase 10** (large-value separation), using Phase 9 as baseline.

## Phase 8: `phase_2_6_some_4`

Goal: keep the strongest low/medium-complexity wins while avoiding recent native regressions.

- Keep `Phase 2` internals unchanged.
- Keep `Phase 6` caches unchanged.
- Keep selective `Phase 4` read-path structure (SST v2 + point-read index + positional reads), but tune block sizing for lower write/index overhead.
- Do not include `Phase 7` compaction changes in this stack.
- Re-benchmark mixed native + wasm for `32/256/4096/1048576` and compare against `Phase 6`.

### Phase 8 implementation diff (code-level)

- Removed Phase 7 WIP behavior from `db.rs` by reverting range-scoped compaction input selection and per-step compaction-budget options.
- Restored compaction behavior to the pre-Phase-7 path (the Phase 6 baseline compaction path).
- Tuned selective Phase 4 block sizing in two passes:
  - Tried `64 KiB` SST blocks + bloom disabled by default; rejected due strong benchmark regressions.
  - Finalized on `32 KiB` SST blocks + bloom enabled by default.
- Removed the optional bloom-disable toggle from `LsmOptions` to keep Phase 8 on a single code path.

## Detailed scope per item

### 1) Mixed workload benchmark suite

- Add shared native + WASM/OPFS scenarios: `mixed_random_70r_30w`, `mixed_random_50r_50w_with_updates`, `mixed_random_60r_20w_20d` (delete-heavy)
- Value sizes: `32`, `256`, `4096`, `1048576` (reduced key count for 1MB)
- Report ops/s and p95 op latency (coarse timing is fine)

### 2) Internal write batching + WAL accounting

- Keep public API unchanged (`put/delete/merge/flush_wal/flush`)
- Implement internal batching/coalescing in WAL append path (no new user-facing write API yet)
- `put/delete/merge` should route through shared append/buffer path
- Track `wal_bytes` in memory; update on append/truncate/replay instead of `file_len()` per write
- Preserve `flush_wal()` semantics

### 3) Buffer reuse

- Replace per-record temporary allocations with reusable encode scratch buffers
- Reuse decode buffers where possible in replay/compaction loops
- Keep format unchanged in this step

### 4) SST v2 block/index read path

- Introduce block-based SST layout (for example 16 KiB data blocks)
- Add lightweight top-level index so point lookup reads only relevant block(s)
- Extend `SyncFs` with positional read API for efficient block fetches
- Keep old SST reader behind compatibility gate if needed; forward format breaks are allowed by project policy

### 5) Bloom filters

- Build per-SST bloom during flush/compaction
- Check bloom before any block fetch in `get()`
- Tune target false-positive rate for browser memory budget

### 6) Caching

- Cache SST index/filter metadata in memory
- Add bounded LRU block cache (configurable bytes)
- Keep cache simple and deterministic for single-thread worker

### 7) Compaction improvements (single-thread cooperative) [Dropped]

- This phase is dropped in its current form.
- Reason: observed native regressions outweighed gains in current measurements.
- If revisited later, it should be treated as a new design phase with a stricter rollback gate.

### 8) `phase_2_6_some_4` consolidation

- Keep Phase 2 and Phase 6 behavior as-is.
- Keep selective Phase 4 read path (SST v2 + point-read index + positional reads).
- Use `32 KiB` SST block target to balance index/block overhead against read amplification.
- Explicitly exclude Phase 7 compaction changes.

### 9) Manifest edits log

- Replace full manifest rewrite with append-only version edits
- Add periodic checkpoint/snapshot of manifest state
- Recovery = checkpoint + tail replay

### 10) Large-value separation

- Add value size threshold option
- Values above threshold stored in append-only blob log
- LSM records keep blob pointer + length + checksum
- Compaction rewrites pointers, not blob payloads

## Non-goals (for this plan)

- Multi-worker write/compaction architecture
- Lock-free or thread-parallel internals
- RocksDB parity across all workloads

## Acceptance gates

Progress to next item only if gates pass:

- Correctness: all native + WASM tests pass
- Durability: `flush_wal()` crash/reopen scenario remains green
- Performance: no regression >10% on existing seq/random microbenches at `32/256/4096`, and mixed workload improvement at current stage is measurable (or change is reverted)

## Notes

- Native production can use RocksDB where available.
- `jazz-lsm` target remains: predictable single-threaded browser engine with materially better mixed workload behavior than current prototype.
