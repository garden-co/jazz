---
"jazz-tools": patch
---

**Rust: text edits together with other column changes.**

- **`jazz.update_edited(id, update, EditedFrom::new().column(tasks::notes(), shown))`** applies the update's text columns named in `EditedFrom` as edits of the values given, and its other columns as plain sets, in one row update, as TS's `update(table, id, data, { editedFrom })` does. Also on `Transaction` and `OpenTransaction`.
