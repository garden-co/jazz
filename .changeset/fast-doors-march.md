---
"cojson": patch
"cojson-core-wasm": patch
"cojson-core-napi": patch
"cojson-core-rn": patch
---

Moved stable JSON serialization from JavaScript to Rust in SessionLog operations

### Changes

- **`tryAdd`**: Stable serialization now happens in Rust. The Rust layer parses each transaction and re-serializes it to ensure a stable JSON representation for signature verification. JavaScript side now uses `JSON.stringify` instead of `stableStringify`.

- **`addNewPrivateTransaction`** and **`addNewTrustingTransaction`**: Removed `stableStringify` usage since the data is either encrypted (private) or already in string format (trusting), making stable serialization unnecessary on the JS side.

