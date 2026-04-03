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
