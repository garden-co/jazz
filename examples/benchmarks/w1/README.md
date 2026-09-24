# W1 wallclock benchmark metadata

[metadata.ts](metadata.ts) documents the memory and RocksDB benchmark cases,
fixtures, timing boundaries and throughput denominators. The executable query
shapes and workload methods live in `src/lib.rs`; the measured closures live in
`benches/reads_memory_walltime.rs` and `benches/reads_rocksdb_walltime.rs`.

These are individual W1-derived operations, not the complete historical mixed
scenario. In particular, task detail performs two reads but is one task-detail
operation, activity table size is not query output size, and a reconnect after
one change is not necessarily a delta-sized response. Keep metadata and the
timer boundary in agreement when changing the harness.

## Permissioned subscription fan-out (#3231)

`src/subscription_fanout.rs` generalizes a dashboard startup pattern: one broad
overview and many independently mounted keyed lists over the same table. Public
builders define teams and tasks with inherited SELECT permission. The overview
has no tenant predicate; the second team's equally sized dataset must be excluded
by authorization. Keyed lists use 60 fixed boards independently of row count.

The topology is three real Jazz runtimes: history-complete Core, scope-isolated
relay and non-durable foreground. Logical in-process transport isolates engine
cost; it is **not** a wire, browser, IndexedDB, React or disk-reload benchmark.
All subscriptions open before pumping, remain live together and consume reset
and delta events until settled. Seeding, runtime creation, connections and teardown
are outside timing. Query preparation, admission and hydration are inside.

CodSpeed cases live in the existing memory walltime target: 600 rows/team with
0, 10 or 60 keyed lists, plus 6,000 rows/team with 60 lists. Row count and binding
count are independent axes. Total stored tasks are twice the rows/team argument.

Standalone attributed receipts (prepare, subscribe, Core/relay/foreground ticks,
total and pump turns) run with:

```sh
cargo run --profile perf -p jazz-example-benchmark-w1 --bin subscription_fanout -- 600 60 3
cargo test -p jazz-example-benchmark-w1 --lib
```

The standalone runner validates exact membership and payloads after timing.
The correctness test also verifies live edits and inherited permission revocation.
The timed benchmark does not replace those tests. Preserve exact source/binary
hashes for comparisons; busy-host local timings are diagnostic, not stable claims.

Experiment premise: previous #2853/#2861 compiler-memo trials did not establish
useful endpoint gains on their cold-load workloads. This workload instead exposes
many independently admitted bindings and measures the complete three-runtime
opening. Any new reuse must preserve identity, claims, read view, catalogue and
receiver scope; disappearance of a compiler frame alone is not acceptance.

The common [metadata contract](../../../dev/benchmarks/metadata/README.md) explains
revision provenance, legacy names, work units and validation.
