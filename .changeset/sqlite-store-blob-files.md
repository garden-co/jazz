---
"jazz-tools": patch
"jazz-napi": patch
---

Large values' bytes can live outside the SQLite file. `sqlite({ path, blobs })` takes `files({ dir? })` (the default, one file per chunk in `<path>/blobs`) or `"sqlite"` (inside the database file, not recommended). The sync server takes `--blobs files|sqlite` (default `files`) and `--blobs-dir`. Rows and the chunk index stay in the database; bytes move out on later ticks, so an existing store moves its large values out by itself. A store keeps its blob store after that: switching back to `"sqlite"` or to another directory is refused as not supported yet. The `vercel()` store's Sandbox core keeps large values in SQLite until it gets a network blob store.
