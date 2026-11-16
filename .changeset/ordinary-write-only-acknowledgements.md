---
"jazz-napi": patch
"jazz-wasm": patch
"jazz-tools": patch
---

A writer now keeps the rows it inserts into a table it cannot read (a write-only insert), and the delete/restore commits after them, as a cold copy once the authority accepts them, including after a later refused edit. Previously the writer's replica dropped them on acceptance. A covering remote read still reconciles that copy away, and a live read of the table still evicts the row so its result keeps matching the authority's. A write that moves a row out of its writer's read scope still evicts the row, with or without a live read, as does another writer's version superseding it. A refused write refreshes the rows its writer may read and no longer evicts the ones it may not.
