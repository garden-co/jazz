# cojson-core-rn

## 2.0.0-alpha.23

## 2.0.0-alpha.22

### Patch Changes

- dedab8f: Add authorship-based edit metadata for row writes across the runtime and bindings.

  Rows now expose `$createdBy`, `$createdAt`, `$updatedBy`, and `$updatedAt` magic columns in queries and permissions, and backend contexts can override stamped authorship with `withAttribution(...)`, `withAttributionForSession(...)`, and `withAttributionForRequest(...)`.

- fd7ecd0: Schema authoring no longer has a build/codegen step. Apps now define their schema directly in TypeScript with the namespaced API (`import { schema as s } from "jazz-tools"`), and `jazz-tools validate` is just an optional local preflight check.

  Current `permissions.ts` is now separate from the structural schema and migration lifecycle, instead of being versioned as part of schema identity.

  Runtime permission enforcement now follows the latest published permissions head independently of client schema hashes, with learned schemas, migration lenses, and permissions rehydrated from the local catalogue on restart.

## 2.0.0-alpha.21

### Patch Changes

- 477c43c: Remove Nitro Modules code (`crates/jazz-nitro`, `examples/rn-jazz-nitro`) in favor of uniffi for the React Native bridge.

## 2.0.0-alpha.20

### Patch Changes

- 9f4d4d9: Bound oversized index keys by keeping as much real value prefix as fits in the durable key and appending a length plus hash overflow trailer.

  This keeps large indexed string and JSON equality lookups working without exceeding storage key limits, while preserving prefix-based ordering instead of collapsing oversized values to a pure hash ordering. Large `array(ref(...))` values also continue to support exact array equality and per-member reference indexing.

## 2.0.0-alpha.19

## 2.0.0-alpha.18

### Patch Changes

- 33bc53f: Fail indexed writes cleanly when an indexed value would exceed the storage key limit instead of panicking in native storage.

  Oversized indexed inserts and updates now return a normal mutation error to JS callers, and local updates can recover rows that were previously left in a partial index state by older panic-driven failures.

## 2.0.0-alpha.17

### Patch Changes

- bb10f1c: Add a shared `jazz-tools/expo/polyfills` entrypoint for Expo apps and ensure published `jazz-rn` packages include the generated C++ bindings required for native builds.

## 2.0.0-alpha.16

## 2.0.0-alpha.15

### Patch Changes

- 4871b02: Switch the native persistent storage engine from SurrealKV to Fjall for the CLI, NAPI bindings, and React Native bindings.

  Native local data now lives in Fjall-backed stores and uses `.fjall` database paths by default.

- bb39e15: Modify inserts to return the inserted row instead of just the id

## 2.0.0-alpha.14

### Patch Changes

- 2f5ccba: Add an in-memory storage driver across the Jazz JS, WASM, NAPI, and React Native runtimes.

  Backend contexts can now opt into memory-backed runtimes without local persistence, and runtime driver-mode coverage was expanded to exercise the new in-memory path.

## 2.0.0-alpha.13

## 2.0.0-alpha.12

## 2.0.0-alpha.11

## 2.0.0-alpha.10

## 2.0.0-alpha.9

## 2.0.0-alpha.8

## 2.0.0-alpha.7

## 0.20.9

## 0.20.8

## 0.20.7

## 0.20.6

## 0.20.5

## 0.20.4

## 0.20.3

## 0.20.2

## 0.20.1

## 0.20.0

### Minor Changes

- 8934d8a: ## Full native crypto (0.20.0)

  With this release we complete the migration to a pure Rust toolchain and remove the JavaScript crypto compatibility layer. The native Rust core now runs everywhere: React Native, Edge runtimes, all server-side environments, and the web.

  ## 💥 Breaking changes

  ### Crypto providers / fallback behavior
  - **Removed `PureJSCrypto`** from `cojson` (including the `cojson/crypto/PureJSCrypto` export).
  - **Removed `RNQuickCrypto`** from `jazz-tools`.
  - **No more fallback to JavaScript crypto**: if crypto fails to initialize, Jazz now throws an error instead of falling back silently.
  - **React Native + Expo**: **`RNCrypto` (via `cojson-core-rn`) is now the default**.

  Full migration guide: `https://jazz.tools/docs/upgrade/0-20-0`

### Patch Changes

- 89332d5: Moved stable JSON serialization from JavaScript to Rust in SessionLog operations

  ### Changes
  - **`tryAdd`**: Stable serialization now happens in Rust. The Rust layer parses each transaction and re-serializes it to ensure a stable JSON representation for signature verification. JavaScript side now uses `JSON.stringify` instead of `stableStringify`.

  - **`addNewPrivateTransaction`** and **`addNewTrustingTransaction`**: Removed `stableStringify` usage since the data is either encrypted (private) or already in string format (trusting), making stable serialization unnecessary on the JS side.

## 0.19.22

## 0.19.19

## 0.19.18

## 0.19.17

## 0.19.16

## 0.19.15

## 0.19.14

### Patch Changes

- 41d4c52: Enabled flexible page-size support for Android builds, enabling support for 16KB page sizes to ensure compatibility with upcoming Android hardware and Google Play requirements for cojson-core-rn.

## 0.19.13

## 0.19.12

## 0.19.11

## 0.19.10

### Patch Changes

- 4f5a5e7: Version bump to align the fixed version

## 0.1.1

### Patch Changes

- d901caa: Added cojson-core-rn that improves ReactNative crypto performance
