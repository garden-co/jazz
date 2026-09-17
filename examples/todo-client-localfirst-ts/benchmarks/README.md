# Todo native workload

[metadata.ts](metadata.ts) is the structured source for the timeline's descriptions,
timing boundaries and throughput units. Update it alongside changes to the harness.

This self-contained Rust variant of the local-first todo example owns the former
native batch-phase workload. Its schema, deterministic rows, driver, assertions,
and profiling support live here. There is no legacy Cargo benchmark target.

The four wall-time cases finish with 1,500 tasks:

- Reopen: open an already seeded RocksDB worker, publish to an empty memory
  foreground, ingest, and query the tasks.
- Batch update: mark 1,350 tasks done in one transaction, including foreground
  authoring, upload encoding, worker ingest and delivery back to the foreground.
- Sequential update: do the same as 1,350 separate transactions.
- Sequential insert: start with 150 seeded, subscribed tasks and insert 1,350
  new unfinished tasks in separate transactions, growing to 1,500. Each insert
  completes the same foreground author/persist, upload, worker ingest/publication,
  encode/decode and foreground application path before the next insert. IDs are
  deterministic caller-supplied IDs; new rows have no predecessor version.

Setup is outside each measured closure. RocksDB uses WAL without fsync. This is
an in-process native worker/foreground workload, not browser or IndexedDB timing;
it excludes JS scheduling and authentication bootstrap. Each measured operation
ends with one read of all tasks, including after the entire sequential loop.
Exact row-ID/completed-ID verification happens when Divan drops the fixture,
outside timing. The diagnostic profile retains its per-delivery verification.

```sh
cargo test -p jazz-example-todo-benchmark --lib
cargo bench -p jazz-example-todo-benchmark --bench walltime --features jazz-benchmark-guard/mimalloc
cargo run -p jazz-example-todo-benchmark --bin todo-profile --profile perf --features jazz-benchmark-guard/mimalloc
JAZZ_TODO_WORKLOAD=sequential-insert cargo run -p jazz-example-todo-benchmark --bin todo-profile --profile perf --features jazz-benchmark-guard/mimalloc
JAZZ_TODO_WORKLOAD=sequential-update cargo run -p jazz-example-todo-benchmark --bin todo-profile --profile perf --features jazz-benchmark-guard/mimalloc
```

The profile binary reports the existing per-phase counters plus a raw storage
baseline. `JAZZ_BATCH_ROWS` and `JAZZ_BATCH_UPDATE_PERCENT` configure it; the
CodSpeed cases have fixed sizes. Keep allocator, optimization profile and machine
architecture equal when comparing numbers. CodSpeed ARM64 results establish a
separate baseline from local x86 measurements.

`JAZZ_TODO_WORKLOAD` defaults to `batch` (the original memory/RocksDB diagnostic
and raw-storage controls). Sequential diagnostics use RocksDB and retain the
existing `batch_*` phase labels for each one-row transaction. Their `rows` field
is the current table size, allowing phase costs to be grouped by growing table
size. `JAZZ_BATCH_ROWS` is the final size for inserts (20..=4096); setup seeds
one tenth of it. Sequential updates honor `JAZZ_BATCH_UPDATE_PERCENT` (default
90). Diagnostic per-delivery verification and printing add work and must not
be compared to clean Divan latencies. Both timed sequential cases do 1,350
transactions, but inserts grow the scope whereas updates keep 1,500 rows
throughout; their timings are not a pure insert-vs-update primitive comparison.

## Hot-row history diagnostic (#2981)

`todo-history-depth` keeps the same todo schema, RocksDB WAL worker, memory
foreground and complete wire roundtrip. It compares `spread` (round-robin
updates) with `hot` (one repeatedly edited row). Both explicitly name the last
authored version of the target row as parent. Optional `stale-parent` instead
names the seed every time: this intentionally creates concurrent siblings and
must not be confused with sequential history depth. Every edit changes the
title and toggles `done`; exact IDs and values are checked after each reporting
window and after reopening the worker and rehydrating a fresh foreground.

```sh
cargo build -p jazz-example-todo-benchmark --bin todo-history-depth --profile perf
JAZZ_HISTORY_ROWS=1500 JAZZ_HISTORY_UPDATES=2000 JAZZ_HISTORY_WINDOW=500 \
  JAZZ_HISTORY_ARMS=spread,hot target/perf/todo-history-depth
```

JSONL windows report phase medians and average logical storage/history reads
per update, plus source and process provenance. Authoring and author persistence
are memory-backed; worker ingest/publication is RocksDB-backed. `upload` and
`publish` include their wire encode/decode. `roundtrip` includes all phases and
measurement bookkeeping, not window validation/printing or fixture setup.
These are diagnostic timings, not CodSpeed cases or browser throughput claims.
The existing four walltime cases and their assertions are unchanged.
