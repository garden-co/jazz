# Permissioned-resource first sync

[metadata.ts](metadata.ts) is the structured source for the timeline's description,
timing boundaries, fixed Member identity and visible-row throughput unit.

Workload revision 2 preserves the existing shallow-history fixture and permission
relationships. Thirty-nine subscriptions produce 27,518 visible rows at full
scale: resources, permission inputs and child rows inheriting their parent's
access. Core, device-local persistence relay and Client use RocksDB, and the client starts empty.
Both device hops use the benchmark reader's identity; Core authorizes the scope,
and the relay retains authorized input versions rather than acting as a server
authority. The relay remains in the measured topology, as in the separate browser
fanout benchmark (whose foreground is non-durable).

The new `first_sync_local_relay_27518_rocksdb` identity starts a fresh baseline.
Do not interpret differences from the retired `first_sync_27518_rocksdb` server-edge
workload as a like-for-like optimization. Its metadata remains for historical runs.

Core seeding is outside the wall-time measurement. Receiver opening, connection,
query preparation, subscription and settling until every expected row is present
are inside. Exact per-table row-ID verification, post-read diagnostic scans and runtime teardown are outside. The
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

Historical reference tools consuming `core-edge.jsonl` / `edge-client.jsonl`
describe the retired topology. New captures are `core-relay.jsonl` /
`relay-client.jsonl`; do not feed these into an old authority model unchanged.

The clean wall-time transport performs one message encode, zstd streaming roundtrip,
and message decode per delivery. It does not run diagnostic codec comparisons.
The diagnostic profile preserves those historical probes; its `wall_ms` includes
post-read diagnostic work and excludes receiver opening. Therefore profile
`wall_ms` and CodSpeed first-sync duration are intentionally different receipts.
