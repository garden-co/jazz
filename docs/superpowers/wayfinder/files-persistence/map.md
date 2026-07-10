# Files persistence — wayfinder map

Label: `wayfinder:map`
Tracker: local-markdown (this directory; tickets are files under `tickets/`)

## Destination

**REACHED (2026-07-10).** A ready-for-agent spec pinning where and how every
piece of file-plane state persists — resolved as: client side, descriptor
column values only (after the invisible-core pivot and the MVP delete cut,
core keeps zero durable file-plane records — no staged bodies, no resume
records, no durable outbox hold, no delete intent); server side, stateless by
prior decisions. Published as the
[Slice 1 spec](../../specs/2026-07-10-files-persistence-spec.md) plus the
amended files PRD (`docs/superpowers/specs/2026-07-09-files-spec.md`,
amendment 9). Nothing is left to decide before implementation tickets can be
cut.

## Notes

- Parent feature spec: `docs/superpowers/specs/2026-07-09-files-spec.md`
  (authoritative on file-plane semantics); human-first companion:
  `docs/superpowers/specs/2026-07-09-files-design-explained.md`.
- Skills per ticket type: `/grilling` + `/domain-modeling` for decisions,
  `/codebase-design` when shaping `Storage`-trait extensions, `/prototype`
  where a concrete artifact would raise fidelity.
- Standing codebase facts (verified 2026-07-09): one synchronous `Storage`
  trait serves client and server
  (`crates/jazz-tools/src/storage/storage_trait.rs:25`); backends RocksDB
  (native/server default), SQLite (RN), custom OPFS B-tree (wasm,
  `crates/opfs-btree`), memory (tests). Durable pending-write state = local
  batch records + sealed submissions + batch fates keyed by `BatchId`
  (`crates/jazz-tools/src/batch_fate.rs`), in the same KV store as rows;
  restart recovery scans them. No separate byte store exists today — large
  blobs persist inline in row batches. New durable state naturally lands as
  `__`-prefixed raw-table namespaces (`raw_table_put/get/scan_prefix`) or new
  trait methods.
- Standing preference: keep artifacts in-repo (no public GitHub issues).

Destination progress: **complete** — all tickets closed, no fog. The spec
pair (slice-1 + amended PRD) is the deliverable; implementation tickets can
be cut from it.

## Decisions so far

<!-- one line per closed ticket: gist + link -->

- [Descriptor persistence](tickets/A-descriptor-persistence.md) —
  `ColumnType::File` facade lowering onto `Value::Text` (canonical JSON,
  `v:1`, strict-write/lenient-read); **no immutability enforcement, no body
  verification** (release = claim + copy out of `pending/`; bucket
  lifecycle TTL replaces the sweep); deletion is an explicit
  uploader-or-backend API, never cell death; cells opaque to queries in v1.
- [Grant ledger](tickets/E-claim-ledger.md) — **there is no ledger, and
  (per the later identity-bound-ids amendment) no bucket-derived checks
  either**: file ids embed the uploader's identity (key
  `{app}[/t{class}]/{identity}/{random}`), so grants and deletes authorize
  by pure identity-segment comparison — zero server state, zero bucket
  reads at issuance, no tombstones, no uploader metadata; takeover
  impossible by construction; TTL classes reinstated (schema-declared,
  class in the id string); URLs publicly carry the uploader's identity id
  (stated). Edges fully stateless — the client holds the `UploadId`. This
  subsumed [Edge grant records](tickets/F-edge-grant-records.md).
- [Explicit-delete execution](tickets/G-deletion-queue.md) — synchronous
  idempotent DELETE in-request, server persists nothing. (Amended by C's
  resolution: the client-persisted intent half was cut from the MVP; the
  stateless server half stands.)
- [Interceptor spike](tickets/H-interceptor-spike.md) — both interceptors
  proven by executed prototypes: SW serves `<img>` loads from OPFS/Cache
  with correct 206 Range synthesis (video-safe), first-load fallthrough
  confirmed, cache puts need `event.waitUntil`; the Rust loopback
  Range-serves off a filesystem dir, loopback-only, secret-gated.
  Handed to B: OPFS raw files favored on web, filesystem dir on
  native/RN, and **SW offline requires `/files/*` on the app's own
  origin** (same-origin interception only).
- [Device file store](tickets/B-staging-store.md) — **the invisible-core
  pivot: core has no device file store.** `fromBlob` uploads in-session
  from the in-memory Blob; a restart mid-upload loses the body and the
  descriptor syncs bodyless (URL 404s) — the documented outcome; `url()`
  is always the public URL. All offline machinery (staging, resume, SW,
  RN loopback, read cache, hook surface) → future opt-in package, out of
  scope; pre-pivot store design preserved in the
  [design inventory](notes/offline-package-inventory.md). PRD amended
  same day (amendment 9: invisible core); slice-1 spec pointers updated.
- [Outbox hold across restart](tickets/D-outbox-hold.md) — it doesn't,
  by design: the hold is an in-memory courtesy; after restart,
  formerly-held transactions sync normally (bodyless descriptor until an
  opt-in package reinstates durable holds). Closed by B's pivot.
- [Pending-delete intent record](tickets/C-resume-records.md) — **no
  record; out of the MVP.** `delete()` returns a Promise (resolves on
  origin confirmation, rejects on failure); retries across restarts are
  the caller's — safe because the DELETE is idempotent. Client-side
  durable file-plane state in core is zero. The deferred record design
  (file-id key, created-at value, `__pending_file_delete` raw table with
  standard header versioning) is preserved in the ticket for whoever
  reinstates it.

## Not yet specified

(Empty — the map is complete. The 2026-07-10 invisible-core pivot moved
all former entries out of scope with the offline package; the last one
(namespace versioning) died with the pending-delete intent record's MVP
cut.)

## Out of scope

- Fate of the legacy large-blob file machinery
  (`packages/jazz-tools/src/runtime/file-storage.ts`, files/file_parts chunk
  rows) — migrate/deprecate/leave is a separate product effort, not
  new-plane persistence.
- **The opt-in offline package** (2026-07-10 pivot, resolved in
  [Device file store](tickets/B-staging-store.md)) — durable staging,
  upload resume, the web SW, the RN loopback server, the read cache, and
  the core hook surface they need. Core stays invisible; any offline
  footprint is added willingly by the app. A future effort, seeded by the
  [design inventory](notes/offline-package-inventory.md); it subsumes the
  former upload-resume half of
  [ticket C](tickets/C-resume-records.md) and all former fog entries
  (crash-consistency test strategy, capacity guardrails, RN staging,
  encryption at rest, interceptor runtime lifecycles).
- Interceptor _implementation_ (production SW fetch-handler, production RN
  loopback server) — was "v1 per the PRD, built from the destination
  spec"; the 2026-07-10 pivot moved it into the opt-in offline package
  above. On-map history: the throwaway feasibility spike (ticket H).
- Per-identity rate limits / quotas (PRD: planned future work).
