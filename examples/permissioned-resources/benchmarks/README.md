# Permissioned-resource first sync

[metadata.ts](metadata.ts) is the structured source for the timeline's description,
timing boundaries, fixed Member identity and visible-row throughput unit.

Workload revision 3 keeps the shallow-history fixture and permission
relationships. Thirty-nine subscriptions produce 27,518 visible rows at full
scale: resources, permission inputs and child rows inheriting their parent's
access through a recursive group policy. Core and Client are file-backed SQLite
databases, and the client starts empty. Core authorizes the benchmark reader's
identity.

Revision 2 (`first_sync_local_relay_27518_rocksdb`) put a device-local
persistence relay between Core and Client, and revision 1
(`first_sync_27518_rocksdb`) a server edge, so `first_sync_direct_27518_file`
is its own baseline, not a like-for-like comparison. Their metadata remains for
historical runs.

Core seeding is outside the wall-time measurement. Client opening, connection,
query preparation, subscription and settling until every expected row is present
are inside. Exact per-table row-ID verification and teardown are outside. The
transport performs one wire encode, zstd roundtrip and decode per frame.

Durable fixtures use Jazz's `open_node_storage` with `RocksDbStorageFactory`
and `WalNoSync`, including cached-seed reopen and receiver opening. Admission
remains within those existing opening phases; receiver admission is therefore
inside first-sync timing. The returned `BoxedStorage` uses the existing dynamic
dispatch path without an additional box. All-memory setup still copies and
verifies every physical seed column family through `OrderedKvStorage`.

```sh
cargo test -p jazz-example-permissioned-resources-benchmark --lib
cargo bench -p jazz-example-permissioned-resources-benchmark --bench walltime --features jazz-benchmark-guard/mimalloc
JAZZ_CUSTOMER_IDENTITY=member JAZZ_CUSTOMER_SCALE=1.0 cargo run --release -p jazz-example-permissioned-resources-benchmark --bin permissioned-resources-profile
```

The profile driver prints one receipt per run: rows, wall, connect, subscribe
and settle times, ticks and compressed bytes sent to the client. Identities are
`member`, `spy` (no grants) and `admin` (sees everything).

`reference/` contains the earlier SQLite/Postgres comparison drivers and
research receipts for revisions 1 and 2.
