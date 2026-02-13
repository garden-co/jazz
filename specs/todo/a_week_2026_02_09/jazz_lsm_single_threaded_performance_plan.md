# jazz-lsm single-threaded performance plan (WASM/OPFS first)

Improve `jazz-lsm` mixed read/write performance in a single-threaded runtime, targeting "good enough in browser" while keeping native usable.

## Constraints

- Single dedicated worker in WASM (`FileSystemSyncAccessHandle` owner)
- Synchronous DB API remains unchanged
- No background threads required for correctness or performance
- Maintenance work must be cooperative (bounded foreground steps)
- Durability contract unchanged: no lost acknowledged writes after `flush_wal()`

## Prioritized roadmap (lowest complexity, highest ROI first)

Order is strict. Stop after each item and re-benchmark.

| Order | Item                                                         | Complexity | Expected mixed R/W impact                                       | Why this order                                                                          |
| ----- | ------------------------------------------------------------ | ---------- | --------------------------------------------------------------- | --------------------------------------------------------------------------------------- |
| 1     | Add mixed workload benchmarks + counters                     | S          | Baseline quality (no direct speedup)                            | Cheapest way to avoid optimizing blind; needed to measure ROI of every next step        |
| 2     | Internal write batching + in-memory WAL byte tracking        | S-M        | +30% to +150% writes, +10% to +60% mixed                        | Removes per-op fixed overhead and avoids repeated WAL length checks                     |
| 3     | Reuse encode/decode buffers in write path                    | S          | +10% to +40% writes, +5% to +20% mixed                          | Allocation churn is currently high and easy to reduce                                   |
| 4     | SST v2 block format + point-read block index                 | M-H        | +3x to +20x reads, +2x to +8x mixed                             | Biggest structural read bottleneck today is full-file reads for point lookups           |
| 5     | Per-SST bloom filters                                        | M          | +1.5x to +4x random reads, +1.3x to +3x mixed                   | Cheap read amplification reduction once block/index format exists                       |
| 6     | SST metadata/index cache + small block cache                 | M          | +1.2x to +3x reads, +1.2x to +2x mixed                          | Avoids repeated parse/read of hot SST internals                                         |
| 7     | Range-scoped compaction picking + compaction budget per op   | M-H        | +1.5x to +4x writes under churn, +1.5x to +3x mixed             | Reduces write amplification and large foreground stalls                                 |
| 8     | Large-value separation (blob log for values above threshold) | H          | +5x to +20x for 1MB workloads, +2x to +8x mixed at large values | High ROI for large values; avoids repeatedly rewriting value payloads during compaction |
| 9     | Append-only manifest edits + periodic checkpoint             | M          | +10% to +40% write-heavy mixed                                  | Lowers metadata rewrite/sync overhead during flush/compaction                           |

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

### 7) Compaction improvements (single-thread cooperative)

- Replace whole-level rewrite with range/file-set selection
- Add configurable per-call compaction budget (bytes or records)
- Run bounded compaction work on foreground operations (`put/get/flush/compact_step`) without long event-loop stalls

### 8) Large-value separation

- Add value size threshold option
- Values above threshold stored in append-only blob log
- LSM records keep blob pointer + length + checksum
- Compaction rewrites pointers, not blob payloads

### 9) Manifest edits log

- Replace full manifest rewrite with append-only version edits
- Add periodic checkpoint/snapshot of manifest state
- Recovery = checkpoint + tail replay

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
