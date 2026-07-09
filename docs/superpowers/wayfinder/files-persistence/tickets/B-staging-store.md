# Device file store (staging + interceptor cache)

Type: `wayfinder:grilling`
Status: open
Assignee: (unclaimed)
Blocked by: [H — interceptor spike](H-interceptor-spike.md)

## Question

Where does the device file store live on each platform — holding both
**staged** bodies (uploads) and the **interceptor read cache** (offline
reads, per the PRD's v1 web-SW + RN-loopback decision) — and with what
crash-consistency contract against the local transaction commit?

There is no byte store today — large blobs persist inline in row batches,
which the PRD forbids for file bodies. Decide: per-platform home (native/RN:
filesystem directory vs blobs in RocksDB/SQLite — must be readable by the
RN loopback server's Rust handler; browser: raw OPFS files vs Cache API —
must be readable from the service worker's context); the key scheme (file
id) and how staged vs cached classes are distinguished; the write order and
failure semantics between "body durably staged" and "descriptor transaction
committed locally" (a crash between the two must not produce a committed
descriptor with no body, nor an unreferenced body that never gets cleaned);
staged-body lifecycle (dropped at acceptance — or demoted to cached? —
cleanup on rejection and on `fromBlob`-but-never-written-to-a-cell); and
cache lifecycle (LRU under a configurable budget, staged bodies exempt
until acceptance, eviction bookkeeping durable enough to survive restarts).
