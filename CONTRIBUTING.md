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
