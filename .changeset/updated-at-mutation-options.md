---
"jazz-tools": patch
---

Add `updatedAt` overrides to `insert`, `update`, and `upsert` mutation options in `jazz-tools`.

The same override is available on the durable variants, so callers can stamp `$updatedAt` explicitly on a per-write basis without changing attribution or session scoping.
