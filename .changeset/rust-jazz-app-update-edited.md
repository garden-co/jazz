---
"jazz-tools": patch
---

**Rust: in-place edits together with other column changes.**

- **`jazz.update_edited(id, update, Edits::new().edited_from(tasks::notes(), shown))`** applies the update's text columns named with `edited_from` as edits of the values given, and its other columns as plain sets, in one row update, as TS's `update(table, id, data, { editedFrom })` does.
- **`Edits::splice`** and **`Edits::json_set`** add splices and json sets to the same update, as TS's `applyDiffs` does. A column can't be both set and diffed.
- Also on `Transaction` and `OpenTransaction`.
