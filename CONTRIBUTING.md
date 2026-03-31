# Contributing

## Prerequisites

### libclang for RocksDB builds

The `rocksdb` feature uses `bindgen` to generate FFI bindings at build time, which requires `libclang`. The default storage backend (`fjall`) does not require this.

**macOS:**

```sh
brew install llvm
```

No additional configuration needed — `.cargo/config.toml` sets the required env vars automatically using Homebrew's stable symlink.

**Linux (Debian/Ubuntu):**

```sh
sudo apt install libclang-dev
```

**Linux (Fedora/RHEL):**

```sh
sudo dnf install clang-devel
```
