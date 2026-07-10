# Device file store (staging + interceptor cache)

Type: `wayfinder:grilling`
Status: closed (resolved 2026-07-10)
Assignee: guido (claimed 2026-07-10)
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

## Resolution (2026-07-10)

**Core has no device file store.** Mid-grilling, the effort pivoted to an
invisible-core stance: offline upload and offline download concerns move
to userland, with Jazz providing opt-in machinery so any extra footprint
is added willingly by the app.

Core behavior:

- `fromBlob` keeps the Blob in memory, measures `size`, and uploads
  in-session — grant → PUT → release — straight from the Blob. No staged
  bodies, no resume records, no staging TTLs, no startup sweeps.
- A restart mid-upload loses the body: the committed descriptor syncs and
  its URL 404s — the ordinary bodyless state, now also the documented
  interrupted-upload outcome. Nothing re-uploads.
- The outbox hold is an in-memory courtesy only; it deliberately does not
  survive restart (closes [D — outbox hold](D-outbox-hold.md)).
- `url()` returns the public URL on every platform — no loopback in core.
- The only durable client-side file-plane record is the pending-delete
  intent (from [G — explicit-delete](G-deletion-queue.md)); its shape
  stays with the slimmed [ticket C](C-resume-records.md).

Everything offline — durable staging, resume, the web SW, the RN loopback
server, the read cache, and the core hook surface they need — is a future
opt-in package, **out of this map's scope**. The store design grilled
before the pivot (OPFS/filesystem home, `staged/`+`cached/` layout,
random-keyed staging minted at `fromBlob`, record-before-body crash
contract with idempotent sweep, demote-at-acceptance) is preserved as the
package effort's starting inventory:
[offline package — design inventory](../notes/offline-package-inventory.md).

PRD amendment flagged: the v1 offline promises (interceptors, offline
creation from day one) become opt-in-package territory; the edit rides
along with the destination spec's publication.
