# Jazz for Swift

The Swift runtime of Jazz: `async`/`await` calls and `AsyncThrowingStream`
subscriptions over `crates/jazz-ffi` (the Jazz core, built from Rust, through
UniFFI). Apps call it through the typed bindings that `jazz-tools generate`
writes as `JazzApp.swift`.

```bash
scripts/build-ffi.sh   # builds libjazz_ffi.a and generates Sources/JazzFFI + Sources/jazz_ffiFFI/include
swift test
```

`scripts/build-ffi.sh` must run before the package builds, and again whenever
`crates/jazz-ffi` changes: the generated sources must match the library. The
library is linked from `target/release/jazz-ffi-static` of this repository,
or from `$JAZZ_FFI_LIB_DIR`.

Built and tested on Linux in CI (`.github/workflows/swift.yml`). iOS apps are
not supported yet. See `examples/stage-plan/apps/swift-cli` for a complete
command-line tool, and the docs' Swift setup page for what's not supported yet.
