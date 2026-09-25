---
"jazz-tools": patch
"jazz-napi": patch
"jazz-wasm": patch
"jazz-rn": patch
---

Use declared composite indexes for Global-tier reads. With an `(owner, rank)` index, a page filtered by `owner` and ordered by `rank` with a `limit` reads only about `limit + 1` index entries and checks deletions for those candidates only, falling back to the ordinary read when it cannot prove the page complete. A first result that filters both columns of a two-column composite reads that index prefix. Results are unchanged, Local-tier reads keep their current path, and schemas without a composite index take none of the new paths.
