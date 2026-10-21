---
"jazz-tools": patch
"jazz-napi": patch
"jazz-wasm": patch
"jazz-rn": patch
---

Add `$deletedAt` and `$deletedBy` as optional projection, filter, and sort columns after `.includeDeleted()`. They return the winning deletion's authored timestamp as a `Date` and author as a `RowAuthor`, or `null` for live and restored rows. History versions share the logical row's current deletion timestamp and author.
