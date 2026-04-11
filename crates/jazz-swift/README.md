# jazz-swift

Thin Apple-platform binding surface for Jazz.

## Purpose

`jazz-swift` is the repo-owned runtime/binding layer for Apple clients.

Keep this package generic to runtime/bootstrap/binding concerns.

App-shaped observable requests, default runtime bootstrap for a specific app,
and typed query helpers belong in a separate `JazzData` package outside this
repo.

## Layout

- `rust/` contains the Rust crate that will expose the low-level binding surface over `jazz-tools`.
- `Package.swift` defines the lightweight Swift package shell used for local package wiring and docs.
- `Sources/JazzSwiftBindings/` holds package-local Swift support code.
- `generated/` is where `uniffi-bindgen` emits Swift sources plus the FFI header and module map.
- `artifacts/ios/` is where built iOS static-library slices are copied for app integration.
- `artifacts/JazzSwiftFFI.xcframework` is the intended packaged native artifact for the app.
- `Tests/JazzSwiftBindingsTests/` provides package-level smoke coverage.

## Rust Surface

The Rust binding mirrors the narrow runtime shape already proven by `jazz-rn`:

- runtime bootstrap with SQLite persistence
- one-shot query
- live `subscribe` / `unsubscribe`
- optional two-phase subscription registration
- `insert`, `update`, and `delete`
- write-context support
- batched-tick scheduling callback
- outbound sync callback
- inbound sync parking
- schema hash, flush, and close helpers

All query parsing, schema alignment, and subscription payload shaping stays in
`jazz-tools`.

## Local Commands

Bootstrap the Apple toolchain:

```sh
bash scripts/install-jazz-swift-deps.sh
```

Verify the Rust crate:

```sh
cargo --config 'net.git-fetch-with-cli=true' test -p jazz-swift
```

Generate Swift bindings from the host build:

```sh
cargo install uniffi --version 0.30.0 --features cli --root target/tools
bash scripts/generate-jazz-swift-bindings.sh
```

Build iOS static-library slices:

```sh
bash scripts/build-jazz-swift-ios.sh
```

Assemble the xcframework after the iOS slices exist:

```sh
bash scripts/build-jazz-swift-xcframework.sh
```

Run generation, iOS builds, and xcframework assembly:

```sh
bash scripts/prepare-jazz-swift-bindings.sh
```

## Notes

- The cargo commands use `net.git-fetch-with-cli=true` because this workspace has
  a patched RocksDB dependency that resolves reliably through the git CLI.
- The Swift package in this repo is intentionally light. The actual app-facing
  data layer should sit above this package in the app repo.
