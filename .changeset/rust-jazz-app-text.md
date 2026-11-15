---
"jazz-tools": patch
---

**Rust: mergeable text editing.**

- **`jazz.edit_text(id, tasks::notes(), edited_from, text)`** (and `tx.edit_text` in a transaction) sets a text column as an edit of the value its writer saw, as TS's `update(…, { editedFrom })` does. On a `"merge": "text"` column, the edit is merged over changes the writer hadn't seen.
- **`JazzClient::update_with_edits`** applies a row update together with large-value edits: text edits, splices and JSON sets.
