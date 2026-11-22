---
"jazz-tools": patch
"jazz-rn": patch
"jazz-napi": patch
"jazz-wasm": patch
---

Add `db.getPendingUploadRows(table)` and `db.onPendingUploadRowsChange(table, listener)`: the ids of a table's rows with a write the server has neither accepted nor refused yet, so an app can show "not sent yet" beside a row. In Node and React Native this includes writes recovered from local storage after a restart. `null` means the runtime cannot tell (no client yet, or a native build without the report), never "none pending".
