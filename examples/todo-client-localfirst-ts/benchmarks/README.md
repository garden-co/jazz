# Todo native workload

This self-contained Rust variant of the local-first todo example owns the former
native batch-phase workload. Its schema, deterministic rows, driver, assertions,
and profiling support live here. There is no legacy Cargo benchmark target.

The three wall-time cases use 1,500 tasks:

- Reopen: open an already seeded RocksDB worker, publish to an empty memory
  foreground, ingest, and query the tasks.
- Batch update: mark 1,350 tasks done in one transaction, including foreground
  authoring, upload encoding, worker ingest and delivery back to the foreground.
- Sequential update: do the same as 1,350 separate transactions.

Setup is outside each measured closure. RocksDB uses WAL without fsync. This is
an in-process native worker/foreground workload, not browser or IndexedDB timing;
it excludes JS scheduling and authentication bootstrap. Each measured operation
ends with one read of all tasks, including after the entire sequential update.
Exact row-ID/completed-ID verification happens when Divan drops the fixture,
outside timing. The diagnostic profile retains its per-delivery verification.

```sh
cargo test -p jazz-example-todo-benchmark --lib
cargo bench -p jazz-example-todo-benchmark --bench walltime --features jazz-benchmark-guard/mimalloc
cargo run -p jazz-example-todo-benchmark --bin todo-profile --profile perf --features jazz-benchmark-guard/mimalloc
```

The profile binary reports the existing per-phase counters plus a raw storage
baseline. `JAZZ_BATCH_ROWS` and `JAZZ_BATCH_UPDATE_PERCENT` configure it; the
CodSpeed cases have fixed sizes. Keep allocator, optimization profile and machine
architecture equal when comparing numbers. CodSpeed ARM64 results establish a
separate baseline from local x86 measurements.
