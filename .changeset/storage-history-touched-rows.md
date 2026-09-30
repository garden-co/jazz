---
"jazz-tools": patch
---

Row history no longer keeps a `by_tx` secondary index; each transaction record lists the rows it touched instead. This saves roughly 250 bytes per row on Core and 125 on a receiver. Row-holding stores declare `jazz.history-version-current.v3`, so a store written by a pre-release build of the linear history layout (`v2`) is refused at open with the typed `UnsupportedStorageCodecs` error. There is no migration: delete the old store.
