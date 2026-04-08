# Vendored RocksDB Archives

Place prebuilt `librocksdb.a` archives here to skip rebuilding RocksDB from C++ sources on supported server targets.

Expected layout:

```text
vendor/librocksdb-sys/prebuilt/<target-triple>/lib/librocksdb.a
```

Supported target triples:

- `aarch64-apple-darwin`
- `x86_64-apple-darwin`
- `aarch64-unknown-linux-gnu`
- `x86_64-unknown-linux-gnu`

Stage an archive into place with:

```sh
bash scripts/stage-vendored-rocksdb.sh <target-triple> /path/to/librocksdb.a
```

The repo-local `librocksdb-sys` patch links these archives when present. If an archive is missing, builds fall back to compiling RocksDB from the upstream `rust-rocksdb` checkout, still using the checked-in bindings in `vendor/librocksdb-sys/bindings/bindings.rs`.
