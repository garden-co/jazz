# Offline package — design inventory

Settled thinking from the 2026-07-10 grilling of
[B — device file store](../tickets/B-staging-store.md), preserved for the
future opt-in offline package effort. These decisions were grilled and
agreed **before** the invisible-core pivot moved the whole store out of
core scope; they are the starting inventory for the package's design, not
binding commitments.

## Scope of the package

Durable staging, upload resume, the web service worker, the RN loopback
server, and the read-through body cache — everything offline. Core ships
none of it; the package hooks into the SDK, so any footprint is added
willingly by the app.

## Store home (was Q1)

- **Web:** OPFS raw files. Cache API ruled out — browser may evict under
  pressure with no exemption (disqualifying for staged bodies, the only
  copy), and its Request/Response model fights Range synthesis, which the
  spike proved clean from OPFS.
- **Native/RN/Node:** a plain filesystem directory. Blobs-in-KV ruled
  out — the loopback server wants seekable Range reads; whole-value KV
  reads of large bodies are the wrong shape.
- Honest durability ceiling: OPFS is best-effort unless
  `navigator.storage.persist()` is granted — same ceiling as the KV store
  itself.

## Layout & key scheme (was Q2)

- Hard constraint: the interceptor read path resolves URL → bytes from
  the file store alone — **no KV lookup** (the SW cannot share the OPFS
  B-tree's single-owner sync access handles).
- Mint the file id's CSPRNG random part at `fromBlob` time; the id is
  still finalized at first cell write (observably identical). Then:
  - `staged/{random}` — named by the id's random part; no rename, no
    mapping, ever. Interceptor extracts the random segment from the URL.
  - `cached/{flat-id}` — full file id flattened (`/` → safe separator);
    cached bodies span other identities, so the full id is required.
- Staged/cached distinction is the directory: eviction scans only
  `cached/`; staged exemption is by construction.
- Serve order: staged → cached → fetch-through (cache write wrapped in
  `event.waitUntil`, per the spike).

## Crash-consistency contract (was Q3)

Ordering plus an idempotent startup sweep — no cross-store transaction:

1. Body writes atomic per platform (OPFS `createWritable()` swap-file
   semantics; native `.part` + fsync + rename), so existence ⇒ complete
   bytes. Same for cache entries.
2. **Record before body:** `fromBlob` durably writes a staging record to
   the KV store (keyed by the random) before writing the body, and
   resolves only after both.
3. Startup sweep: record without complete body → drop record (+ `.part`
   debris); `staged/` file without record → delete; record still in
   `local` older than a **staging TTL** (order-of-days, mirroring the
   grant lease / `pending/` lifecycle philosophy) → drop both.
4. Body loss after descriptor commit (storage eviction, cleared site
   data) is a defined terminal state: upload fails on the handle, hold
   releases, descriptor syncs bodyless (URL 404s).

Net: complete staged body ⇔ staging record, repaired at startup;
descriptor ⇒ body staged, by ordering. Testable by crash-point injection
at each write boundary.

## Staged-body lifecycle (was Q4)

- **Acceptance → demote to cache** (`staged/{random}` →
  `cached/{flat-id}`, fresh LRU recency): rename on native, OPFS `move()`
  where supported, streaming copy-then-delete elsewhere (Firefox/Safari
  lack `move()`). Idempotent, driven by the staging record's state,
  resumed by the sweep. Then ordinary cache: evictable, refetchable.
- **Rejection → delete staged body + record**, no demotion; the bucket
  object is left alone (deletion is never a side effect).
- **Never written to a cell →** staging TTL.
- **Lease-expiry restart** (fresh id ⇒ fresh random): re-key the staged
  body to the new random (move or copy); rare path (days-long offline).

## Open when the package effort starts

- Cache-eviction bookkeeping (LRU under a configurable budget, durable
  across restarts) — never grilled.
- Upload-resume record shape and home (relocated out of ticket C when it
  was slimmed to the pending-delete intent). Soft input: SW-readability
  of the record (e.g. a sidecar file beside the staged body) would enable
  Chromium Background Sync retries, at the cost of KV transactional
  coupling.
- The core hook surface: staging interception around `fromBlob`/upload,
  resume re-entry, a URL-rewrite point for the RN loopback `url()`.
- OS background uploads (NSURLSession background transfer / WorkManager)
  are afforded by staging-as-plain-files: background upload tasks require
  a file on disk. iOS uploads whole files per task — multipart parts need
  pre-split part files or single-PUT-only in background.
- Capacity guardrails, RN staging specifics, encryption at rest, SW
  registration/update lifecycle, loopback port/secret lifecycle — all
  former map fog that left scope with the package.
