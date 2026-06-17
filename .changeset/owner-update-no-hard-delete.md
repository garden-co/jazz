---
"jazz-tools": patch
"jazz-wasm": patch
"jazz-napi": patch
"jazz-rn": patch
---

Fix an owner `db.update` of a backend-created row hard-deleting the row instead of updating it on persistent-storage clients. A client write can no longer downgrade a batch the server has already accepted, so the row survives and the update applies.
