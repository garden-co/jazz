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

### libclang for RocksDB builds

The `rocksdb` feature uses `bindgen` to generate FFI bindings at build time, which requires `libclang`.

**macOS:**

```sh
brew install llvm
```

Then symlink `libclang.dylib` into `/usr/local/lib` so the dynamic linker can
always find it, regardless of how the build is invoked:

```sh
sudo mkdir -p /usr/local/lib
sudo ln -s "$(brew --prefix llvm)/lib/libclang.dylib" /usr/local/lib/libclang.dylib
```

> Note: environment variables like `DYLD_LIBRARY_PATH` are stripped by macOS
> SIP in many process chains, so the symlink approach is more reliable.

**Linux (Debian/Ubuntu):**

```sh
sudo apt install libclang-dev
```

**Linux (Fedora/RHEL):**

```sh
sudo dnf install clang-devel
```

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
