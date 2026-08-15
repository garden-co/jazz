# Async page-store microbenchmark

This is a narrow browser microbenchmark of the same async Rust B-tree over two
page-addressed backing stores: IndexedDB and OPFS. IndexedDB is **not** used as
an ordered KV store: the tree can only ask either adapter for opaque numeric
pages and commit opaque page records. Tree descent, ordering, splitting, scan,
and cache eviction are all in `AsyncOpfsBTree`.

## Result

Five Chromium worker repeats, median operations/s. The value size is inline
payload bytes. `range` is queries/s and always returns exactly 32 rows; its
returned-row rate is in parentheses.

| value | backing store | sequential put, checkpoint/256 | shuffled put, checkpoint/256 | warm single-key get | cold random get (clean reopen) | cold random range, 32 rows | 90/10 read/write mixed, checkpoint/256 |
| ----: | :------------ | -----------------------------: | ---------------------------: | ------------------: | -----------------------------: | -------------------------: | -------------------------------------: |
|  32 B | IndexedDB     |                         44,594 |                        9,736 |             167,594 |                          4,725 |      2,348 (75,142 rows/s) |                                  4,766 |
|  32 B | OPFS          |                         37,509 |                        8,730 |             167,869 |                          2,990 |      1,510 (48,313 rows/s) |                                  2,931 |
| 256 B | IndexedDB     |                          8,814 |                        1,321 |             112,941 |                          2,488 |        577 (18,465 rows/s) |                                  3,071 |
| 256 B | OPFS          |                         16,725 |                        3,015 |             111,709 |                          1,590 |        375 (11,995 rows/s) |                                  2,148 |

The cache-hot single-key path is effectively tied, as expected: after preload it
does not access either backing store. At 32 B IndexedDB led every
storage-sensitive path (about 1.1–1.6x). At 256 B OPFS led the checkpointed
write paths (about 1.9x sequential and 2.3x shuffled), while IndexedDB led
reads, ranges, and the mixed workload (about 1.4–1.7x). The 256 B IndexedDB
write results drifted through the alternating series, so use that write
crossover as directional rather than a stable production claim.

## Method

- 4 KiB page codec, 3-page B-tree cache, 4,096 keys, fixed LCG shuffle, and
  identical async tree/store configuration for both adapters.
- Each measured write and mixed phase checkpoints after every 256 operations;
  the timed write path therefore includes backing-store commits rather than
  retaining all dirty pages in memory.
- Cold reads/ranges free and reopen the tree immediately before timing. With a
  three-page cache, shuffled 4,096-key access forces B-tree page cache misses.
  Warm reads preload and repeatedly read one fixed root-to-leaf path, which fits
  the current tree shape's three-page cache, outside the timer.
- Every timed phase is at least 100 ms; the only phase that repeated a batch to
  reach that minimum records its actual operation count in the raw receipt.
- Backend order alternates per repeat. Exact raw elapsed times, operation
  counts, and derived rates are in [ASYNC_PAGE_STORE_RAW_RESULTS.md](ASYNC_PAGE_STORE_RAW_RESULTS.md).

Run after building the WASM benchmark bindings:

```sh
pnpm --dir crates/opfs-btree run bench:wasm:build
pnpm --dir crates/opfs-btree run test:wasm:async-page-stores
```

## Scope and durability

The browser assertion covers both stores at the tree boundary: an awaited
`put(k, v)` is immediately observable through `get(k)` before checkpoint, and
the value survives `checkpoint`, `free`, and reopen. IndexedDB groups a
checkpoint into one relaxed `readwrite` transaction and is atomic at that
transaction boundary. The experimental OPFS adapter writes and flushes the same
pages but does **not** provide multi-page crash atomicity. Neither result
establishes crash recovery, end-to-end Jazz performance, or a production
durability guarantee.

The async B-tree is deliberately incomplete: no delete/rebalance, no overflow
values, and no WAL/recovery protocol. These numbers are therefore useful only
for choosing a future page backing-store direction, not as a production storage
benchmark.
