# opfs-btree: WASM + OPFS-only engine plan

## Scope

Design and implement a browser-first B+tree engine for Dedicated Worker + OPFS sync handle usage.

Key constraints:

- WASM + OPFS only (Dedicated Worker)
- Async open, then synchronous API
- Single OPFS file (avoid multi-file coordination)
- Read-first optimization: point reads and range scans are primary
- Writes can be slower; durability can be checkpoint-based (no instant per-write durability required)

## Why B+tree over current LSM path

Given read-mostly, mostly-small-value workloads with acceptable write latency:

- B+tree gives bounded read path (single root-to-leaf traversal) and straightforward range scans.
- No compaction machinery and fewer moving parts than read-optimized LSM.
- Single-file page layout maps naturally to OPFS sync access.

## Runtime model

- Dedicated worker owns `FileSystemSyncAccessHandle`.
- `open()` is async (obtain handle, read metadata, warm caches).
- All data operations are sync once open.
- Main-thread and other tabs communicate via message passing to the tab leader worker.

## File layout (single file)

- Page size: 16 KiB (initial default, configurable later).
- Page 0: superblock A
- Page 1: superblock B
- Remaining pages: B+tree pages + overflow pages + freelist pages.

Page kinds:

- `Internal`
- `Leaf`
- `Overflow` (optional for large values)
- `Freelist`

## Checkpoint protocol: double superblock root-pointer swap

Superblock fields:

- magic/version
- generation (monotonic)
- root page id
- freelist head page id
- total pages
- checksum

Commit steps:

1. Write modified tree/freelist pages.
2. Flush file handle.
3. Write inactive superblock with new generation and root/freelist pointers.
4. Flush file handle.

Recovery/open:

- Read both superblocks.
- Validate checksums/version.
- Pick highest valid generation.
- If one is torn/corrupt, use the other.

This avoids mandatory WAL for v1 and keeps recovery constant-time.

## In-memory structures

- Page cache (LRU, byte budget)
- Dirty page set
- Root page id
- Optional leaf cursor cache for range locality
- Optional hot-key hint table persisted in superblock extension pages (phase 3+)

## API (v1)

```rust
pub struct BTreeOptions {
    pub page_size: usize,
    pub cache_bytes: usize,
    pub overflow_threshold: usize,
}

pub struct OpfsBTree<F: SyncFile> { ... }

impl<F: SyncFile> OpfsBTree<F> {
    pub fn open(file: F, options: BTreeOptions) -> Result<Self, Error>; // called after async OPFS open
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, Error>;
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error>;
    pub fn delete(&mut self, key: &[u8]) -> Result<(), Error>;
    pub fn range(&mut self, start: &[u8], end: &[u8], limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Error>;
    pub fn checkpoint(&mut self) -> Result<(), Error>;
}
```

Notes:

- `SyncFile` is a swappable sync file interface (Memory/Std/OPFS adapters).
- WASM wrapper provides async handle acquisition and then constructs `OpfsBTree`.

## Initial page encoding choices

- Fixed page header with kind, item count, sibling pointers (leaf), and checksum.
- Slotted page layout for variable-length keys/values.
- Prefix compression in leaf keys (phase 2, after correctness).
- Values above `overflow_threshold` stored in overflow pages (phase 2).

## Performance goals

- Fast open: read two superblocks + root page + minimal metadata.
- Fast point reads: <= 1 root + tree depth pages, mostly cache hits for hot set.
- Fast range scans: leaf linked-list traversal with sequential page reads.

## Rollout phases

### Phase 1 (this implementation start)

- New crate scaffold.
- Sync file abstraction.
- Single-file page IO helpers.
- Double-superblock encode/decode + open/selection.
- Checkpoint writer for superblock swap.
- Tests for torn/corrupt superblock recovery behavior.

### Phase 2

- Minimal functional B+tree (leaf/internal split, insert/get/delete/range).
- Freelist page allocator.
- Overflow pages for large values.

### Phase 3

- Cache tuning, prefix compression, leaf prefetch for range.
- Startup hot-leaf hints.
- Benchmarks against current opfs-btree wasm/opfs baseline profiles.

### Phase 4

- Optional lightweight intent log for faster crash-window recovery between checkpoints.
- Background checkpoint policy integration in tab-leader runtime.

## Risks and mitigations

- Risk: write amplification from full-page rewrites.
  - Mitigation: bounded dirty-set checkpoint cadence and overflow values.
- Risk: fragmentation from overflow/freelist churn.
  - Mitigation: extent-aware freelist bins and periodic page defrag pass.
- Risk: long sync stalls in worker.
  - Mitigation: chunked checkpoint and cooperative scheduling hooks.

## Acceptance criteria for phase 1

- Valid superblock swap with generation monotonicity.
- Recovery chooses latest valid superblock when one is torn/corrupt.
- Reopen after checkpoint restores root/freelist pointers exactly.
- Tests pass on native; wasm compile for crate succeeds.

## Current implementation status

- Done: crate scaffold, `SyncFile` abstraction (`MemoryFile`, `StdFile`), superblock A/B codec, generation-based recovery, checkpoint slot swap.
- Done: phase 2 baseline page engine:
  - page-structured B+tree nodes (`Internal`/`Leaf`) with split-on-insert
  - overflow page chains for large values
  - freelist page persistence and reuse
  - sync API (`get`/`put`/`delete`/`range`) over the in-memory page graph
  - checkpoint writes page set + freelist metadata, then superblock root swap
- Done: regression test for the two-crash pattern where only checkpointed state survives.
- Next: phase 3 performance work (cache tuning, prefix compression, scan prefetch, benchmarks).
