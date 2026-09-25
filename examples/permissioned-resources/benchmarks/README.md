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

The local profile's warm phase closes and reopens the persistence relay. Its row
cache survives, but known state never survives a node restart. Reopen checks
therefore require no known-state declaration and exact resulting row membership;
they are not same-process reconnect measurements.

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

## Initial SELECTs (fresh runtime for every sample)

Two hosted Divan cases cover direct reads separately from first synchronization:

- `initial_selects_39_tables_27518_rows_rocksdb`: 39 unbounded SELECTs, 27,518 rows.
- `initial_selects_39_tables_limit100_879_rows_rocksdb`: the same 39 queries with
  LIMIT 100, returning 879 rows in total.

Both reuse the full-scale fixture, fixed Member identity and table order. Before
**every sample**, `with_inputs` copies the seeded RocksDB store, opens a new
runtime with seeded node 1, and prepares all queries. The timed method executes
only their first reads at Global / Deferred / LocalOnly and retains the results.
Calling it twice on the same runtime fails, preventing silent warm-plan reuse.
OS caches may be warm; "initial" refers to the runtime and query plans, not a
cold filesystem cache. No deletion/history or recovery workload is added.

Divan drops returned results after timing. That drop checks each table's exact
UUID-ordered membership against the independent fixture oracle, encodes the
complete results and writes `INITIAL_SELECT_RECEIPT` JSON with per-table byte
lengths/hashes to stderr. Those hashes are diagnostic comparison values, not
wire/storage identifiers. Database close and temp-directory removal happen
afterward. The new cases retain all query outputs until the end of the sweep;
the native profiling lane times queries separately and encodes/drops each
output between queries, so their aggregate timings should not be conflated.

Run just these cases with:

```sh
cargo bench -p jazz-example-permissioned-resources-benchmark --bench walltime --features jazz-benchmark-guard/mimalloc -- initial_selects_
```

For per-query phase attribution, the existing native driver remains available:

```sh
JAZZ_CUSTOMER_INITIAL_SELECTS=1 JAZZ_CUSTOMER_REOPEN_SEEDED_NODE=1 JAZZ_CUSTOMER_PHASES=cold JAZZ_CUSTOMER_NO_DIAGNOSTICS=1 cargo run -p jazz-example-permissioned-resources-benchmark --bin permissioned-resources-profile --profile perf --features cold-settle-attribution
```

Add `JAZZ_CUSTOMER_QUERY_LIMIT=100` for its page lane. Its copy/open/prepare and
encoding stay outside each reported read duration. Compare instrumented
binaries only with the same instrumentation.

These benchmark IDs start new series. Historical optimization comparisons need
the same benchmark source on both revisions, with exact result signatures
checked; an absent baseline is not an improvement. The benchmark PR is placed
below the read optimizations so CodSpeed can establish a common harness before
comparing them. Existing first-sync timing and IDs are unchanged. See #3541.

Tooling-friction: the old hosted suite covered first synchronization but omitted
the direct first-read endpoint, while repeated local reads hid cold lowering.
