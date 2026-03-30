# Contributing

## Prerequisites

### macOS: libclang for RocksDB builds

The `rocksdb` feature depends on `librocksdb-sys`, which uses `bindgen` to generate Rust FFI bindings from C++ headers at build time. `bindgen` requires `libclang.dylib` to be findable on the library search path.

On macOS with Homebrew's LLVM, `libclang.dylib` lives under `/opt/homebrew/Cellar/llvm/<version>/lib` but isn't on the default search path. Without it, builds fail with:

```
libclang.dylib not found
```

**Fix:** add these to your shell profile (`~/.zshrc` or `~/.bash_profile`):

```sh
export DYLD_LIBRARY_PATH=/opt/homebrew/Cellar/llvm/19.1.7_1/lib${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}
export LIBCLANG_PATH=/opt/homebrew/Cellar/llvm/19.1.7_1/lib
```

Adjust the LLVM version (`19.1.7_1`) to match what Homebrew installed. You can find it with:

```sh
ls /opt/homebrew/Cellar/llvm/
```

Then restart your terminal or run `source ~/.zshrc`.

> **Note:** This only affects local development with the `rocksdb` feature enabled. CI has LLVM installed in standard locations. The default storage backend (`fjall`) does not require libclang.
