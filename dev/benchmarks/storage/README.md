# Storage Benchmarks

Shared raw key/value storage benchmark assets live here.

- `bench-core/` declares benchmark profiles, deterministic phase operation
  streams, the `BenchEngine` trait, fixture decoding, and the browser runner.
- `data/` contains committed `.kv` fixtures shared by browser and native
  storage benchmarks.
- `native/` contains Criterion benchmarks comparing native `rusqlite`, RocksDB,
  and redb. It covers native durable adapters only.

Run the shared core tests:

```bash
cargo test -p jazz-storage-bench-core
```

Run the native storage benchmarks:

```bash
cargo bench -p jazz-storage-native-bench --bench native_storage_engines
```

Browser persistence performance is covered by the IndexedDB browser benchmark; it
depends on this shared core and copies fixtures from `data/` at Trunk build
time.
