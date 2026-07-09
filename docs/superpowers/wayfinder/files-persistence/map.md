# Files persistence — wayfinder map

Label: `wayfinder:map`
Tracker: local-markdown (this directory; tickets are files under `tickets/`)

## Destination

A ready-for-agent spec pinning where and how every piece of file-plane state
persists — client side (descriptor column values, staged bodies, upload-resume
records, the outbox hold) and server side (the core's permanent claim ledger,
edge grant records, the core's deletion queue) — consistent with the files PRD
(`docs/superpowers/specs/2026-07-09-files-spec.md`). Done when nothing is left
to decide before implementation tickets can be cut.

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

## Decisions so far

<!-- one line per closed ticket: gist + link -->

(none yet)

## Not yet specified

- Crash-consistency contract and test strategy across the two stores (KV
  transaction vs staged body write) — sharpens once B and C settle.
- Storage schema versioning/migration story for the new namespaces —
  sharpens once A/C/E pick their encodings.
- Staging-store capacity guardrails (device pressure, many parallel uploads)
  — sharpens once B settles.
- React Native staging specifics, if B resolves browser+native first.
- Whether staged bodies need encryption at rest on any platform.

## Out of scope

- Fate of the legacy large-blob file machinery
  (`packages/jazz-tools/src/runtime/file-storage.ts`, files/file_parts chunk
  rows) — migrate/deprecate/leave is a separate product effort, not
  new-plane persistence.
- The service-worker offline-read integration itself (PRD: future work);
  only its constraint on B (staged bodies must live where a SW can read)
  is on this map.
- Per-identity rate limits / quotas (PRD: planned future work).
