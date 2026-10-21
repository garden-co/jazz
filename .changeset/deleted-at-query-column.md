---
"jazz-tools": patch
"jazz-napi": patch
"jazz-wasm": patch
"jazz-rn": patch
---

Add `$deletedAt` as an optional projection, filter, and sort column after `.includeDeleted()`. It returns the winning deletion's authored timestamp as a `Date`, or `null` for live and restored rows. History versions share the logical row's current deletion timestamp.
