---
"jazz-tools": patch
---

In an exclusive transaction, `insert` with an explicit row id is now create-only: it rejects with `WriteRejected` instead of replacing a row that already exists, is deleted, or was already written for that table earlier in the same transaction. Use `upsert` (or `update`) to overwrite. An insert over a row hidden from the caller by read policy returns the same error as an `upsert` over it, so the rejection does not reveal that the row exists.
