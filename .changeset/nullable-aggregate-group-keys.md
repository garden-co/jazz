---
"jazz-tools": patch
"jazz-napi": patch
"jazz-wasm": patch
"jazz-rn": patch
"jazz-rn-ios": patch
"jazz-rn-android": patch
---

Fix nullable group keys having different public representations in aggregate reads and subscriptions. One-shot reads now preserve NULL group keys and use the same logical result types as snapshots and live updates.

Fix live aggregates losing nullable groups when their source rows change. Updates now find the indexed group using the same nullable key encoding, preserving counts, sums and group identity.
