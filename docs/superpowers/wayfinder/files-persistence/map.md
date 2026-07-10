# Files persistence — wayfinder map

Label: `wayfinder:map`
Tracker: local-markdown (this directory; tickets are files under `tickets/`)

## Destination

A ready-for-agent spec pinning where and how every piece of file-plane state
persists — client side (descriptor column values plus, after the 2026-07-10
invisible-core pivot, a single pending-delete intent record: core keeps no
staged bodies, no resume records, no durable outbox hold) and server side
(stateless by prior decisions) — consistent with the files PRD
(`docs/superpowers/specs/2026-07-09-files-spec.md`), which the pivot amends:
offline upload/download move to a future opt-in package. Done when nothing is
left to decide before implementation tickets can be cut.

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

Destination progress: the resolved half is published as the ready-for-agent
[Slice 1 spec — file column & stateless file plane](../../specs/2026-07-10-files-persistence-spec.md);
the open tickets below cover the rest.

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
  idempotent DELETE in-request, server persists nothing; the SDK persists
  a **pending-delete intent** locally and retries across restarts until
  origin confirms (permanent denials drop it; calls dedupe; promise
  resolves on confirmation). Intent record's shape/home belongs to
  [C — upload-resume records](tickets/C-resume-records.md).
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

## Not yet specified

(Empty — the 2026-07-10 invisible-core pivot moved all former entries out
of scope with the offline package, except namespace versioning, which
folded down into [Pending-delete intent record](tickets/C-resume-records.md).
Remaining route: resolve C, then publish the destination spec.)

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
