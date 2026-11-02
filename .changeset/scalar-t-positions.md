---
"jazz-tools": patch
---

Store and sync every row version at one ordered position instead of a (node, seq) pair. Unsettled writes now order last-writer-wins by their commits' hybrid clocks, so a worker and its tabs agree on concurrent edits even without a server. An explicit write time is recorded as `$updatedAt` but no longer orders the write, and a peer's clock is counted no more than 500 ms ahead of this store's own and never shifts the `$createdAt`/`$updatedAt` this store stamps. A tab that reconnects to a different or wiped worker drops the old worker's unsettled writes. A worker without a server keeps its tabs' writes pending until a server is added, and a serializable transaction waits for that server instead of being validated by the worker. Local stores and the sync wire format from earlier builds of this alpha are not readable by this one: delete the local store and resync.
