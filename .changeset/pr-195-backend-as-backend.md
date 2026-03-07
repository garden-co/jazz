---
"jazz-tools": patch
---

Add `asBackend()` for server-side Jazz clients using backend-secret auth, and enforce backend-role limits so backend sync can write row data but cannot write schema/permissions catalogue entries.
