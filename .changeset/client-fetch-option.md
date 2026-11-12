---
"jazz-tools": patch
---

`createJazzClient({ fetch })` opens again: the sign-in `fetch` (and the account `store`) stay with the account manager instead of reaching the client's configuration, where `db.getConfig()` threw `DataCloneError`.
