---
"jazz-tools": patch
"jazz-napi": patch
"jazz-wasm": patch
"jazz-rn": patch
---

Add `$deletedAt` and `$deletedBy` as optional projection, filter, and sort columns after `.includeDeleted()`. They return the winning deletion's authored timestamp as a `Date` and author as a `RowAuthor`, or `null` for live and restored rows. History versions share the logical row's current deletion timestamp and author.

Filters on `isNull`, `eq: null` and `ne: null` now become null checks for every column type before any type-specific handling. Author columns such as `$createdBy` previously threw on `eq: null`; they now match rows whose author is null.

`$deletedAt` and `$deletedBy` are rejected in flat joins: `.includeDeleted()` widens only the root query, so joined sources have no deletion metadata.

The query compiler now treats `$createdAt` and `$updatedAt` like other ordered scalar columns: a join on either can drive an indexed lookup, and an equality on either against a query parameter becomes a binding-route key instead of a residual predicate. Results are unchanged. Structured author columns stay predicates.
