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

### RocksDB build requirements

RocksDB is compiled from source on the first build (cached by `sccache` afterwards).
This requires a working C/C++ toolchain and `libclang` for `bindgen`:

- macOS: `xcode-select --install` is enough.
- Linux: install `libclang-dev` (Debian/Ubuntu) or `clang-devel` (Fedora).

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
