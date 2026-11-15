---
"jazz-tools": patch
---

**Rust: large values.**

- **`jazz.insert_streaming(new, column, chunks)`**, **`update_streaming(id, update, column, chunks)`** and **`upsert_streaming(id, new, column, chunks)`** write a bytes or text column from an async `Stream` of chunks, like TS's `insertStreaming`. Each chunk is stored as it arrives, between the client's other calls, so the value is never held whole. A failing stream abandons the upload with `Error::Upload`.
- **`jazz.splice(id, column, Page, splices)`** and **`jazz.json_set(id, column, edits)`** (and `tx.splice` / `tx.json_set`) edit bytes, text and json cells in place, like TS's `update(…, { applyDiffs })`.
- **`JazzClient` streaming uploads:** `begin_streaming_upload`, `push_streaming_upload`, `finish_streaming_upload` and `abort_streaming_upload` drive the core's resumable uploads.
- **Not supported yet:** streaming inside a transaction, and edits combined with whole-column replacements in one update.
