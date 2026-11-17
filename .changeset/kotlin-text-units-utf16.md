---
"jazz-tools": patch
---

jazz-kotlin: text positions are UTF-16 code units (Kotlin's `String` indices) by default. `Edits.splice(column, from, to, …)` splices a text column in UTF-16 without naming the units, as `Column.range` already reads; `SpliceUnits.Utf8` and `SpliceUnits.Bytes` stay available.
