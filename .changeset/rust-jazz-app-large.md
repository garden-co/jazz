---
"jazz-tools": patch
---

**Rust: large values.**

- **`jazz.insert_streaming(new, column, reader)`** inserts a row with its bytes column read from a `std::io::Read`, like TS's `insertStreaming`.
- **`jazz.splice(id, column, Page, splices)`** and **`jazz.json_set(id, column, edits)`** edit bytes, text and json cells in place, like TS's `update(…, { applyDiffs })`.
