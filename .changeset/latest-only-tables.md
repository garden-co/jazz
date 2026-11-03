---
"jazz-tools": patch
---

A table can declare `.history("latest")` to keep only each row's latest version, for rows overwritten often whose past is worthless, such as editor presence. Such a table can't be read through `history`, at a snapshot or in a serializable transaction, and can't use `branchBy`, text-merging columns or values large enough to be chunked. Switching an existing table to latest-only drops its history for good, so the first `createJazz` deployment that does it must pass `allowDroppingHistory: true`.
