# Large-value subtraction baseline

Status: complete. This document records the removal checklist for the deliberately
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

- [x] Remove `LargeValueKind`, `ColumnSchema::large_value`, descriptor/catalogue
      encoding, schema hashes, public schema conversions, WASM descriptors, and TS
      DSL/codegen/schema loading support.
- [x] Remove `Value::LargeValue` / `LargeValueHandle` handling, binding codec
      variants, public API types, and NAPI/WASM materialization APIs.
- [x] Keep plain `Value::String`, `Value::Bytes`, arrays, refs, and their normal
      constraints intact.

### Core write/read/merge behavior

- [x] Remove `text_merge`, `text_oplog`, large-value edit commits and metrics,
      handle encode/decode/materialization/cache, special merge/diff logic, and
      missing-extent retry behavior.
- [x] Remove content store, extents, chunk/bundle/checkpoint records, eviction
      accounting, and storage schemas that exist exclusively for them.
- [x] Remove large-value query lowering/output sources and special query errors.

### Protocol and sync

- [x] Remove content extent ownership/version entries and
      `FetchContentExtent`/`ContentExtents` messages from protocol, peer, DB
      transport, ingest, server shell, codecs, wire fixtures, and limits.
- [x] Bump the intentionally breaking wire version and regenerate golden frames.

### Surface, documentation, and tests

- [x] Delete feature-only Rust/TS tests, fixtures, benches, simulations,
      quarantine/invariant rows, examples, docs, and CI/gate references.
- [x] Remove the former chapter 12 and all normative cross-references; replace
      only with a short explicit "not in this core baseline" note where necessary.
- [x] Preserve ordinary byte/file examples only where they use plain columns;
      otherwise remove them from this baseline.

### Verification

- [x] Repository source search has no active large-value/extent/oplog machinery.
      Admin catalogue parsing intentionally recognizes legacy `large_value` and
      `large` keys only to reject them with a schema error.
- [x] Cargo workspace and TypeScript compile gates, focused affected tests, wire
      contracts, formatting, invariant registry, and sensitive-data guard pass.
      The full Jazz runtime suite still encounters the independently reproduced
      base-branch structured-subscription hang; this cut does not add a new
      failure set.

## Commit plan

1. This inventory/spec contract.
2. Remove schema/value/API/protocol vocabulary and feature-owned modules.
3. Remove core paths, bindings, query sources, and generated wire fixtures until
   the ordinary core compiles.
4. Remove dependent tests, benchmarks, examples, docs, invariants, and stale
   dependencies; run broad verification.

## Mechanical cut map

The legacy feature is not confined to one crate or a single value enum. These
are the source-owning boundaries to remove together, in dependency order:

1. `crates/jazz/src/schema.rs`, `protocol.rs`, `protocol_limits.rs`,
   `node/{content_store,text_oplog}.rs`, `text_merge.rs`, `merge_strategy.rs`,
   `json_merge.rs`, and `markdown_strategy.rs` define the feature vocabulary.
2. `node/mod.rs`, `node/ingest.rs`, `node/query_eval.rs`,
   `node/query_engine/{input,output,schemas}.rs`, `node/physical.rs`,
   `node/views.rs`, `peer.rs`, and `db.rs` consume that vocabulary on the core
   read/write/sync paths. The cut must preserve their ordinary row path rather
   than merely deleting imports.
3. `tools/{client,public_schema,admin_catalogue_payload_codec,server/**}.rs`,
   `crates/jazz-napi`, `crates/jazz-wasm`, and `packages/jazz-tools/**` expose
   it through bindings, schema DSL/codegen, and browser/native file helpers.
4. `SPEC/**`, `INVARIANTS.md`, sim/bench targets, examples, fixtures, and
   generated wire frames document or test it and must be removed after the
   compiler has no feature consumer left.

The current cut cannot safely be made by deleting the owning files first:
legacy materialization helpers are interleaved with ordinary catalogue, cache,
and subscription methods in `node/mod.rs`; ingestion has the same shape. Use
compiler-driven, function-bounded edits and a compile gate after each boundary.

## Intentional non-goals

- No migration path for serialized large values: this branch intentionally breaks
  the new-core wire/storage format.
- No content-addressed/object-store primitive yet.
- No userland documents, streams, files, ropes, JSON parts, or benchmark
  replacements in this subtraction PR.
