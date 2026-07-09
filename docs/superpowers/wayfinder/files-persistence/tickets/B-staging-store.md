# Staging store for bodies

Type: `wayfinder:grilling`
Status: open
Assignee: (unclaimed)
Blocked by: (none)

## Question

Where do staged bodies live on each platform, and with what
crash-consistency contract against the local transaction commit?

There is no byte store today — large blobs persist inline in row batches,
which the PRD forbids for file bodies. Decide: per-platform home (native:
filesystem directory vs blobs in RocksDB; browser: raw OPFS files vs Cache
API — the PRD requires somewhere a future service worker can read; React
Native: filesystem vs SQLite blobs), the key scheme (file id), the write
order and failure semantics between "body durably staged" and "descriptor
transaction committed locally" (a crash between the two must not produce a
committed descriptor with no body, nor an unreferenced body that never gets
cleaned), and when staged bodies are dropped (PRD: at acceptance) including
cleanup on rejection and on `fromBlob`-but-never-written-to-a-cell.
