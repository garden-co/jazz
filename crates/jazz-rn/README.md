# jazz-rn

React Native Turbo Module for the Jazz database runtime. Rust owns the database,
transactions, queries, subscriptions, sync protocol, and Groove SQLite storage;
TypeScript owns the public Jazz API and WebSocket carrier. Hermes never loads
WASM and no JavaScript SQLite driver is involved.

## Architecture

- One dedicated Rust actor thread owns each thread-affine `Db`.
- UniFFI `Send + Sync` handles marshal work to that actor.
- The generated bindings expose the full `NativeDb` contract consumed by
  `NativeRuntimeAdapter`, including transactions, permission probes, waiters,
  subscriptions, and batched wire frames.
- Persistent opens use bundled SQLite and deterministically reuse node/author
  identity. Reconnect bootstraps restored pending uploads synchronously.
- Closing the database cancels waiters, checkpoints storage, and joins the actor.

The package contains an XCFramework for iOS device and simulator slices and JNI
libraries for Android arm64-v8a, armeabi-v7a, x86, and x86_64. iOS has a minimum
deployment target of 15.1; Android artifacts target API 23 and use 16 KiB-safe
linker settings.

## Build and test

From the repository root:

```bash
cargo test -p jazz-rn
pnpm --filter jazz-rn typecheck
pnpm --filter jazz-rn test
dev/gates/rn-bindings-fresh.sh
pnpm --filter jazz-rn ubrn:ios
pnpm --filter jazz-rn ubrn:android
```

`ubrn:*` rebuilds the Rust artifacts and regenerates the TypeScript/C++ bridge.
The freshness gate rebuilds the host library and checks both committed generated
TypeScript files without requiring an iOS or Android toolchain.
The iOS command sets `IPHONEOS_DEPLOYMENT_TARGET=15.1`; Android requires an NDK
configured for the React Native project.

Applications must install `jazz-rn` directly, rebuild their native project after
upgrades, and use `jazz-tools/react-native`. See
`examples/todo-client-localfirst-expo` for the persistence and sync E2E.
