# Permissioned-resource first sync

Workload revision 1 preserves the existing shallow-history fixture and permission
relationships. Thirty-nine subscriptions produce 27,518 visible rows at full
scale: resources, permission inputs and child rows inheriting their parent's
access. Core, Edge and Client use RocksDB, and the client starts empty.

Core seeding is outside the wall-time measurement. Receiver opening, connection,
query preparation, subscription and settling until every expected row is present
are inside. Post-read diagnostic scans and runtime teardown are outside. The
local profile driver uses the same fixture and execution path, then collects its
additional phase, storage and memory diagnostics.

```sh
cargo test -p jazz-example-permissioned-resources-benchmark --lib
cargo bench -p jazz-example-permissioned-resources-benchmark --bench walltime --features jazz-benchmark-guard/mimalloc
JAZZ_CUSTOMER_PHASES=cold cargo run -p jazz-example-permissioned-resources-benchmark --bin permissioned-resources-profile --profile perf --features jazz-benchmark-guard/mimalloc
```

Existing `JAZZ_CUSTOMER_*` environment controls remain available for local profile
experiments. CodSpeed fixes scale, identity, topology and subscription cadence;
its name includes the full result cardinality. Its ARM64 baseline is separate
from local x86 measurements. The `perf` profile used for detailed local CPU
attribution is also distinct from Cargo's optimized `bench` profile.

Allocation and phase instrumentation remain opt-in features (`bench-alloc-metrics`,
`bench-alloc-sites`, `bench-perf-control`, `cold-settle-attribution`). Instrumented
receipts must not be presented as clean wall-time measurements.

`repro.sh` runs the identity matrix. `reference/` contains the SQLite/Postgres
comparison drivers and historical research receipts. No workload implementation
remains in `crates/jazz-sim/benches` or `dev/benchmarks`; generic simulation helpers
remain in `jazz-sim`.
