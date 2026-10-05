---
"jazz-tools": patch
"jazz-wasm": patch
"jazz-napi": patch
"jazz-rn": patch
---

Preserve the original content schema and transaction when a frozen read projects retained rows through a table rename. Exclusive reads can obtain valid authority coverage without relabelling immutable history, refreshing their snapshot or weakening concurrent-write conflict detection.
