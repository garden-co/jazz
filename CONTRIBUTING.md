# Contributing

## Prerequisites

### libclang for RocksDB builds

The `rocksdb` feature uses `bindgen` to generate FFI bindings at build time, which requires `libclang`. The default storage backend (`fjall`) does not require this.

**macOS:**

```sh
brew install llvm
```

Cargo does not support making repo-local `.cargo/config.toml` `[env]` entries
conditional on macOS, so keep this setup user-local. First, get the Homebrew
LLVM lib directory:

```sh
brew --prefix llvm
```

Then use that path in either your shell profile or your personal
`~/.cargo/config.toml`:

```sh
# ~/.zshrc or ~/.bashrc
export LIBCLANG_PATH="/opt/homebrew/opt/llvm/lib"
export DYLD_LIBRARY_PATH="/opt/homebrew/opt/llvm/lib"
```

```toml
# ~/.cargo/config.toml
[env]
LIBCLANG_PATH = { value = "/opt/homebrew/opt/llvm/lib", force = false }
DYLD_LIBRARY_PATH = { value = "/opt/homebrew/opt/llvm/lib", force = false }
```

Replace `/opt/homebrew/opt/llvm/lib` with the path from `brew --prefix llvm` if
your Homebrew install lives elsewhere.

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
