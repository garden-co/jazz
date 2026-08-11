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

### Pre-commit hooks in restricted shells

The staged Rust hook receives its file list directly from Lefthook and invokes
Cargo through `dev/scripts/clippy-staged.sh`. It asks Cargo for authoritative
workspace metadata once, then invokes Cargo directly; this avoids Node
child-process invocations of `git` and `cargo`, which can be denied by
sandboxed shells while ordinary Git commands still work. Run
`pnpm test:tooling` to exercise workspace-member, standalone, excluded, and
failure paths. Nonmember/auxiliary crates deliberately fall back to the root
workspace all-targets guard; maintained standalone crates should have their
own explicit gates. Run `pnpm test:tooling:real` for the slower real-Cargo
probe of that fallback. Direct invocations of
`node dev/scripts/clippy-staged.mjs` remain available for local debugging.

### Running tests

```sh
pnpm test          # everything (via turbo)
cargo test -p jazz --no-default-features --features test   # rust core only
```

### Bounded Rust runs and receipts

Use the repository launcher when a test could hang or when comparing local and
CI run time. It writes a machine-readable JSON receipt containing the exact
command, exact dirty-tree fingerprint (without source contents), toolchain,
cache configuration, shard, timing, and direct exit status.

```sh
# Recommended: per-test slow timeout, named hung-test output, deterministic shard.
cargo install cargo-nextest --locked
node dev/gates/run-rust-tests.mjs --shard-index 1 --shard-count 2 -- \
  --workspace --lib --bins --tests --features test

# No Devbox or Nextest required: preserves Cargo selection and adds an overall
# timeout, but cannot attribute a hang to an individual test.
node dev/gates/run-rust-tests.mjs --timeout-seconds 900 -- -p jazz
```

The Nextest `jazz` profile reports a test slow after 60 seconds and terminates
it one minute later. Hash partitions are deterministic and do not overlap for a
fixed test inventory; keep the shard count identical across all CI shards.

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
cargo test -p jazz --no-default-features --features test

# Review each pending change interactively — shows a diff, asks accept/reject
cargo insta review

# Or accept all pending snapshots at once (when you trust the new output)
cargo insta accept
```

`cargo insta review` rewrites the `@"..."` string in the source file directly.
No git-tracked `.snap` files to manage.
