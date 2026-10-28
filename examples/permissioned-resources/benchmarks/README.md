# Permissioned-resource first sync

[metadata.ts](metadata.ts) is the structured source for the timeline's description,
timing boundaries, fixed Member identity and visible-row throughput unit.

Workload revision 3 keeps the shallow-history fixture and permission
relationships. Thirty-nine subscriptions produce 27,518 visible rows at full
scale: resources, permission inputs and child rows inheriting their parent's
access through a recursive group policy. Core and Client are file-backed SQLite
databases, and the client starts empty. Core authorizes the benchmark reader's
identity.

`first_sync_local_relay_27518_file` runs the same workload through the
device-local native relay: Core serves a fresh file-backed relay over the same
wire, and the client is the relay's in-memory foreground, subscribed on the
relay's owner thread as on a device. The harness ticks Core and carries frames;
the relay drives itself.

`first_sync_bundle_27518_file` is an experimental fast initial load: Core
writes the reader's replica for the same subscriptions as a ready-made SQLite
file (`Db::export_initial_bundle`), the client opens it after a zstd roundtrip,
connects, and resumes each subscription from the bundle's seq, so only
support rows travel over the wire. The measurement includes building the
bundle. `JAZZ_CUSTOMER_TOPOLOGY=bundle` in the profile driver prints its
phases.

Revision 2 (`first_sync_local_relay_27518_rocksdb`) put a RocksDB persistence
relay between Core and a RocksDB client, and revision 1
(`first_sync_27518_rocksdb`) a server edge, so neither is a like-for-like
comparison with revision 3. Their metadata remains for historical runs.

Core seeding is outside the wall-time measurement. Client opening, connection,
query preparation, subscription and settling until every expected row is present
are inside. Exact per-table row-ID verification and teardown are outside. The
transport performs one wire encode, zstd roundtrip and decode per frame.

```sh
cargo test -p jazz-example-permissioned-resources-benchmark --lib
cargo bench -p jazz-example-permissioned-resources-benchmark --bench walltime --features jazz-benchmark-guard/mimalloc
JAZZ_CUSTOMER_IDENTITY=member JAZZ_CUSTOMER_TOPOLOGY=relay JAZZ_CUSTOMER_SCALE=1.0 cargo run --release -p jazz-example-permissioned-resources-benchmark --bin permissioned-resources-profile
```

The profile driver prints one receipt per run: rows, wall, connect, subscribe
and settle times, ticks and compressed bytes sent to the client. Identities are
`member`, `spy` (no grants) and `admin` (sees everything); topologies are
`direct` and `relay`.

`reference/` contains the earlier SQLite/Postgres comparison drivers and
research receipts for revisions 1 and 2.
