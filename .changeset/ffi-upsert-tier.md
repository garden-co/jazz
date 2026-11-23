---
"jazz-tools": patch
---

Kotlin `upsert` takes an optional `tier`, like `insert` and `update`: with `WriteTier.Global` it returns once the sync server accepted the write. jazz-ffi gains `upsert_at` (the C ABI's `upsert` already took `"tier"`; it is documented now), and an upsert at a tier returns the row as written, so an accepted write is no longer reported as refused when someone deletes or hides the row before the server's answer arrives.
