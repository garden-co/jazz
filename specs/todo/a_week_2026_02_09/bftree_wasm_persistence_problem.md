# BfTree WASM persistence: fundamental viability problem

## Finding

BfTree's persistence model relies on two mechanisms working together:

1. **WAL** (write-ahead log) — cheap append-only buffer flushed to disk, captures every write
2. **Snapshot** — expensive full serialization of the tree to disk, after which the WAL can be safely truncated

On **native**, the lifecycle is:
- Open: load snapshot → replay WAL → create fresh WAL (offset 0)
- Runtime: writes go to WAL (background flush thread)
- Shutdown: `snapshot()` writes full tree state to disk, WAL is implicitly truncated on next open

This works because `snapshot()` persists everything, so the WAL is ephemeral by design — it only needs to survive between snapshots.

## The WASM problem

`snapshot()` **cannot work on WASM**. It calls `CircularBuffer::drain()` which enters a multi-threaded spin loop (`try_bump_head_address_to_evicting_addr` in a `loop` with `backoff.snooze()`). On WASM's single-threaded runtime, this is an infinite busy-wait that blocks the event loop and hangs the worker.

Without `snapshot()`:
- WAL replay loads data into memory on open ✓
- But `open_with_opfs` creates a fresh WAL at `file_offset: 0`, overwriting the old WAL
- Since there's no snapshot, the replayed data only lives in memory
- On the next reload, both WAL and snapshot are empty → data lost

### Why "just keep appending to the WAL" isn't a solution

An ever-growing WAL without compaction will:
- Grow without bound (every mutation appended forever)
- Make recovery increasingly slow (replay everything since first write)
- Eventually exceed OPFS quota

The WAL is architecturally a *delta log between snapshots*. Without snapshots, it becomes the only source of truth, which it was never designed to be.

## Root cause

BfTree's `CircularBuffer` and `snapshot()` are designed for multi-threaded native environments. The eviction/drain machinery uses locks, condition variables, and spin-wait patterns that assume other threads can make progress concurrently. None of this works on WASM's single-threaded, cooperative-multitasking runtime.

## Options

1. **Make snapshot work on WASM** — Rewrite `CircularBuffer::drain()` to not spin-wait. This is deep in bf-tree internals and may require rethinking the eviction design for single-threaded contexts.

2. **Bypass CircularBuffer for WASM snapshots** — Write a WASM-specific snapshot path that directly serializes the tree without going through the CB drain. The tree structure (page table, inner nodes, leaf nodes) is all accessible; the CB drain is an optimization for evicting cached pages to disk, which may not be needed when we're writing everything anyway.

3. **Replace BfTree on WASM** — Use a simpler key-value store that doesn't need multi-threaded buffer management. Options include a sorted log-structured store, a simple B-tree without the circular buffer, or direct OPFS key-value serialization.

4. **Hybrid: WAL with periodic compaction** — Keep the WAL-only approach but add a compaction step that rewrites the WAL as a fresh minimal log (current state only, no history). This avoids the snapshot machinery but requires writing a custom compaction pass.

## Current state

- Writes reach BfTree in-memory ✓
- `flush_wal()` writes WAL to OPFS ✓
- WAL replay on open recovers data ✓
- But fresh WAL overwrites old entries → data survives exactly one reload
- `snapshot()` hangs on WASM → cannot use the designed persistence path
