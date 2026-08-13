# Large-value subtraction baseline

Status: in progress. This document is the removal checklist for the deliberately
breaking new-core baseline. It removes the existing specialized `Text`/`Blob`
large-value feature completely; it does **not** introduce a replacement document,
file, stream, rope, content-store, or compatibility layer.

## Contract after this change

- Schema columns are ordinary Groove scalar/array/reference columns only.
- `string` and `bytes` stay ordinary values. Their history is ordinary Jazz row
  history.
- The sync protocol transports ordinary commit, schema, query, and subscription
  data only. It has no extent fetch, content delivery, parking, or checkpoint
  traffic.
- The core has no text/blob edit API, materialized-handle value, special merge
  strategy, content store, or large-value query source.
- Existing feature-specific examples, tests, benchmarks, and docs are removed
  rather than silently reinterpreted. Future replacement feature PRs stack on
  this baseline.

## Inventory and removal checklist

### Canonical schema and values

- [ ] Remove `LargeValueKind`, `ColumnSchema::large_value`, descriptor/catalogue
      encoding, schema hashes, public schema conversions, WASM descriptors, and TS
      DSL/codegen/schema loading support.
- [ ] Remove `Value::LargeValue` / `LargeValueHandle` handling, binding codec
      variants, public API types, and NAPI/WASM materialization APIs.
- [ ] Keep plain `Value::String`, `Value::Bytes`, arrays, refs, and their normal
      constraints intact.

### Core write/read/merge behavior

- [ ] Remove `text_merge`, `text_oplog`, large-value edit commits and metrics,
      handle encode/decode/materialization/cache, special merge/diff logic, and
      missing-extent retry behavior.
- [ ] Remove content store, extents, chunk/bundle/checkpoint records, eviction
      accounting, and storage schemas that exist exclusively for them.
- [ ] Remove large-value query lowering/output sources and special query errors.

### Protocol and sync

- [ ] Remove content extent ownership/version entries and
      `FetchContentExtent`/`ContentExtents` messages from protocol, peer, DB
      transport, ingest, server shell, codecs, wire fixtures, and limits.
- [ ] Bump the intentionally breaking wire version and regenerate golden frames.

### Surface, documentation, and tests

- [ ] Delete feature-only Rust/TS tests, fixtures, benches, simulations,
      quarantine/invariant rows, examples, docs, and CI/gate references.
- [ ] Remove the former chapter 12 and all normative cross-references; replace
      only with a short explicit "not in this core baseline" note where necessary.
- [ ] Preserve ordinary byte/file examples only where they use plain columns;
      otherwise remove them from this baseline.

### Verification

- [ ] Repository source search has no active large-value/extent/oplog machinery
      (historical release notes are the only allowed residual mentions).
- [ ] Cargo and TypeScript compile/test gates, wire contract/goldens, formatting,
      smoke benchmark harness, invariant registry, and sensitive-data guard pass.

## Commit plan

1. This inventory/spec contract.
2. Remove schema/value/API/protocol vocabulary and feature-owned modules.
3. Remove core paths, bindings, query sources, and generated wire fixtures until
   the ordinary core compiles.
4. Remove dependent tests, benchmarks, examples, docs, invariants, and stale
   dependencies; run broad verification.

## Intentional non-goals

- No migration path for serialized large values: this branch intentionally breaks
  the new-core wire/storage format.
- No content-addressed/object-store primitive yet.
- No userland documents, streams, files, ropes, JSON parts, or benchmark
  replacements in this subtraction PR.
