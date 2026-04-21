# Contributing

## Prerequisites

### sccache (strongly recommended)

[sccache](https://github.com/mozilla/sccache) caches compiler invocations across feature sets, profiles, and branches. Without it, crates with heavy C dependencies (rocksdb) recompile on every build.

```sh
cargo install sccache   # or: brew install sccache
```

Then add to your shell profile (`~/.zshrc`, `~/.bashrc`, etc.):

```sh
export RUSTC_WRAPPER=sccache
```

### RocksDB artifact cache

The repo-local `librocksdb-sys` patch now uses checked-in bindings, so `libclang`
is no longer required for RocksDB builds.

Supported server targets now download a pinned prebuilt RocksDB archive from GHCR
into the local Cargo cache on the first build, then reuse it on later builds.

The cache root defaults to:

```text
$CARGO_HOME/jazz-cache/rocksdb/<manifest-digest>/<target-triple>/lib/librocksdb.a
```

Supported prebuilt target triples:

- `aarch64-apple-darwin`
- `x86_64-apple-darwin`
- `aarch64-unknown-linux-gnu`
- `x86_64-unknown-linux-gnu`

Override the cache root with:

```sh
export JAZZ_ROCKSDB_CACHE_DIR=/path/to/cache-root
```

The default GHCR package is public, so the fast path works without credentials.
If the package ever becomes private again, export credentials first:

```sh
export JAZZ_ROCKSDB_GHCR_USERNAME=your-github-username
export JAZZ_ROCKSDB_GHCR_PASSWORD=your-read-packages-token
```

`GHCR_USERNAME` plus `CR_PAT` works too. Without credentials, builds fall back to
compiling RocksDB from source.

To rebuild and publish the full supported set on macOS, use:

```sh
bash scripts/publish-rocksdb-artifacts.sh
```

If an archive is missing or GHCR fetch is unavailable, builds fall back to
compiling RocksDB from the upstream `rust-rocksdb` checkout, which still needs a
working C/C++ toolchain.

## Testing

### Running tests

```sh
pnpm test          # everything (via turbo)
cargo test -p jazz-tools --features test   # rust core only
```

### Snapshot testing with insta in rust

Sync integration tests use [insta](https://insta.rs) for inline snapshot assertions. Snapshots live directly in the test source as `@"..."` strings — no separate `.snap` files.

```rust
insta::assert_snapshot!(tracer.tally(), @"
alice    -> server  : ObjectUpdated (1)
server   -> alice   : PersistenceAck (2)
");
```

When a snapshot doesn't match, the test fails and insta records the new value. To review and update:

```sh
# Install the insta CLI (once)
cargo install cargo-insta

# Run the failing tests
cargo test -p jazz-tools --features test

# Review each pending change interactively — shows a diff, asks accept/reject
cargo insta review

# Or accept all pending snapshots at once (when you trust the new output)
cargo insta accept
```

`cargo insta review` rewrites the `@"..."` string in the source file directly.
No git-tracked `.snap` files to manage.
