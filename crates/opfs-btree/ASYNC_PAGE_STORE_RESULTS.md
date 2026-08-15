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

| value | backing store | sequential put, checkpoint/256 | shuffled put, checkpoint/256 | warm random get (16-key working set) | cold random get (clean reopen) | cold random range, 32 rows | 90/10 read/write mixed, checkpoint/256 |
| ----: | :------------ | -----------------------------: | ---------------------------: | -----------------------------------: | -----------------------------: | -------------------------: | -------------------------------------: |
|  32 B | IndexedDB     |                         46,048 |                       10,220 |                                4,758 |                          4,753 |      2,371 (75,874 rows/s) |                                  4,795 |
|  32 B | OPFS          |                         39,309 |                        8,769 |                                3,131 |                          2,950 |      1,499 (47,969 rows/s) |                                  2,946 |
| 256 B | IndexedDB     |                          8,795 |                        1,308 |                                2,690 |                          2,504 |        582 (18,618 rows/s) |                                  3,094 |
| 256 B | OPFS          |                         16,570 |                        2,966 |                                1,681 |                          1,573 |        373 (11,938 rows/s) |                                  2,185 |

At 32 B IndexedDB led every measured path (about 1.2–1.6x). At 256 B OPFS led
the checkpointed write paths (about 1.9x sequential and 2.3x shuffled), while
IndexedDB led reads, ranges, and the mixed workload (about 1.4–1.7x). The 256 B
IndexedDB write results drifted through the alternating series, so use that
write crossover as directional rather than a stable production claim.

## Method

- 4 KiB page codec, 3-page B-tree cache, 4,096 keys, fixed LCG shuffle, and
  identical async tree/store configuration for both adapters.
- Each measured write and mixed phase checkpoints after every 256 operations;
  the timed write path therefore includes backing-store commits rather than
  retaining all dirty pages in memory.
- Cold reads/ranges free and reopen the tree immediately before timing. With a
  three-page cache, shuffled 4,096-key access forces B-tree page cache misses.
  Warm reads first populate the 16-key working set outside the timer.
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

The contract tested here is awaited-write visibility to following program-order
reads and a clean subsequent reopen. IndexedDB groups a checkpoint into one
relaxed `readwrite` transaction and is atomic at that transaction boundary.
The experimental OPFS adapter writes and flushes the same pages but does **not**
provide multi-page crash atomicity. Neither result establishes crash recovery,
end-to-end Jazz performance, or a production durability guarantee.

The async B-tree is deliberately incomplete: no delete/rebalance, no overflow
values, and no WAL/recovery protocol. These numbers are therefore useful only
for choosing a future page backing-store direction, not as a production storage
benchmark.
