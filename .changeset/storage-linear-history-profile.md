---
"jazz-tools": patch
---

Stores that hold rows now declare the `jazz.history-version-current.v4` storage codec family (linear row-state history). A native, relay or Core store written by alpha.54 to alpha.57 is refused when it is opened, with a typed `UnsupportedStorageCodecs` error naming the missing and unknown codec families, instead of opening and failing later with a record decode error. There is no migration: delete the old store (or keep the previous release) until a migration decision is made. Browser IndexedDB stores are refused by the existing manifest check. The server's account registry and catalogue-entry store are unchanged.
