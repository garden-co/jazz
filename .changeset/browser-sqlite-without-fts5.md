---
"jazz-tools": patch
---

The browser WASM is smaller: its SQLite no longer includes FTS5, R-Tree, the session extension, or diagnostic virtual tables. Jazz uses none of them. Raw SQL in the browser that uses FTS5, `rtree`, `dbstat` or `sqlite_offset()` now fails with SQLite's "no such module" or "no such function" error.
