---
"jazz-tools": patch
---

**Rust: partial reads of large values.** `query.part(column, ValuePart::…)` reads a column only in part, like TS's partial select. That covers a byte range of a bytes column, a UTF-8 or UTF-16 range of a text one, or the subtree at a JSON pointer of a json one. Only the chunks the part covers are read. A column a computed field is computed from is read whole, so the computed field stays right, as TS re-reads it in full. A part inside an include's query reads the included rows' column in part. Generated bindings also allow `unused_variables`, so a table with computed fields and no transforms compiles under `-D warnings`.
