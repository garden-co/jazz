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

The common [metadata contract](../../../dev/benchmarks/metadata/README.md) explains
revision provenance, legacy names, work units and validation.
