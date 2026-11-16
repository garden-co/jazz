---
"jazz-tools": patch
---

**Rust: streaming uploads inside a transaction.**

- **`tx.insert_streaming` / `update_streaming` / `upsert_streaming`** on `Transaction` and `OpenTransaction` stream a bytes or text column into a row staged in the transaction, as TS's `tx.insertStreaming` does. The row commits with the transaction and rolls back with it; an upload dropped mid-stream is aborted and leaves the transaction usable.
- **`JazzClient::begin_streaming_upload`** now works on a transaction's client: `finish_streaming_upload` stages the row in that transaction (returning no commit id) instead of refusing.
