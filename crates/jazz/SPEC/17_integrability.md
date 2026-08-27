# jazz — Specification · 17. Integrability roadmap

## Overview

This chapter is the implementation roadmap for making jazz easy to embed from
TypeScript, WebAssembly, native Node, and server deployments while preserving the
normative contracts in chapters 2–16. It is an index of milestones and ownership:
operational details live in the chapters that own the semantics.

Invariant digest: no `INV-*` ids are defined or cited by this chapter.

## Details

### 17.1 Target outcome

The target is one coherent product surface:

- browser apps call a TypeScript API backed by WASM;
- Node apps call the same TypeScript API backed by NAPI when available;
- servers run a small operational shell around `Node`, not a wider client `Db`;
- edge and core deployments are topology choices, not separate products;
- branch views, lenses, subscriptions, and storage expose stable facades instead of
  leaking reference-implementation internals.

This roadmap does not redefine jazz semantics. When there is tension, the data
model, transaction, sync, topology, API, lowering, branch-view, lens, and maintained
subscription chapters win.

### Package-boundary direction

The intended dependency direction is a featureless semantic core with thin
runtime, target, storage, server, binding, telemetry, CLI, and test shells
depending inward. A boundary earns its cost by clarifying semantic ownership,
target ownership, or feature selection; splitting crates merely because a file
is large, or because it might improve a serial build, is not sufficient.
Native and WASM targets cannot share compiled artifacts across triples. The
remaining package work is tracked in [#1773](https://github.com/garden-co/jazz/issues/1773).

### 17.2 P0 — lock the integration contract

Milestone: **one runnable local app can use the public TypeScript API through
WASM or NAPI against a server shell without semantic forks.**

**Implementation status.** The NAPI/server vertical slice is covered by
`opens, mutates one row, and queries it through the native runtime payload shape`,
`propagates an edge-tier query over the native runtime/server boundary and returns remote row adds`,
and `server_command_loads_published_schema_and_persists_ws_data_across_restart`.
The capability matrix and the complete product-facing lens/branch-view facades remain
roadmap work.

- **TS/WASM/NAPI boundary and capability matrix** — ch. 13 owns the primary direct
  named-call / event ABI over `Db` and selected binding-facing `Node` methods,
  descriptor/raw `Record` row payloads, errors, cross-binding capability matrix,
  and binding-facing sync hooks.
- **Wire protocol** — ch. 8 owns the versioned envelope, feature bits, auth
  claims, replay/idempotency, canonical fixtures, and transport state machine.
- **Immediate P0 blockers** — close parameter binding for direct calls and query
  inputs, define sessioned simulation/admission over `WireSession`, and publish
  canonical ABI/wire fixtures consumable by Rust and TypeScript before treating
  the integration contract as frozen.
- **Server shell and deployment roles** — ch. 9 owns the role ladder, server
  shell responsibilities, topology conformance, edge/cache behavior, and
  deployability knobs.
- **Authorization/session identity** — ch. 7 owns account/user/session/system
  terminology, admission hooks, claims, backend attribution, and fail-closed
  policy behavior.
- **Storage portability** — groove ch. 2 owns the portable ordered-KV backend
  contract; jazz ch. 14 owns which jazz data lowers to that substrate.
- **Subscription event bridge** — ch. 16 owns maintained subscription terminal
  deltas and the TypeScript event bridge; ch. 13 owns the ergonomic watch API.
- **Lens/branch-view facades** — ch. 10 and ch. 11 own schema lenses and normalized
  head/base branch sources; ch. 13 owns their product-facing API placement.

The former launch binding TODOs are folded into this roadmap. Go, Swift,
Kotlin, React Native, NAPI, and WASM are language/package surfaces over the same
capability matrix, wire fixtures, storage configuration, and error vocabulary.
The first supported binding must prove the shape; later bindings should consume
the same fixtures instead of each inventing a parallel runtime contract.

#### 17.2.1 NAPI implementation status and next practical step

`jazz-napi` exists as a workspace `cdylib` crate and Node package sibling to
`jazz-wasm`. It is built with napi-rs for Linux x64 gnu, Windows x64 MSVC, macOS
x64, and macOS arm64 targets. Release workflows build the per-platform `.node`
artifacts, stage napi-rs platform packages under `crates/jazz-napi/npm/*`, wire
them into the root loader as optional dependencies, verify scoped
`@garden-co/*` package names, and publish the loader plus platform packages in
lockstep with `jazz-tools`, `jazz-wasm`, and `create-jazz` alpha versions.
Preview builds reuse the same assembled package artifact.

The binding shape remains the same contract as WASM: idiomatic host objects
around the real Rust `Db`, transactions, subscriptions, and transports. It must
reuse core payloads such as `ReadOpts`, `Error`, and `WireError`, and call
postcard directly where a byte payload is useful; it must not recreate a
command/event runtime inside Rust.

The next useful NAPI milestone is no longer "create the package"; it is a
package-level conformance canary that opens a native `Db`, runs
create/update/delete/query flows, exposes one subscription as a host
stream/callback, proves the row-record decoder shape used by WASM examples, and
exercises the same WebSocket/server boundary as the browser worker gate.

**Implementation status.** `opens, mutates one row, and queries it through the
native runtime payload shape` and `delivers native NAPI subscription updates
through the native handle` exercise the current package-level native runtime
surface.

### 17.3 P1 — harden deployability

Milestone: **a browser client, Node client, edge node, and core node can run the
same conformance scenarios with topology-specific configuration only.**

- **Edge topology.** Implement deployment profiles for client, relay, edge, and
  core roles. Role flags decide fate authority, durability guarantees, caching,
  and eviction; protocol behavior stays shared.
- **Conformance matrix.** Add black-box tests that run the same API scenarios
  against Rust-only, WASM, NAPI, browser-worker, local server, and edge/core
  layouts. Cover mergeable/exclusive transactions, RLS,
  subscription deltas, branch views, and lenses.
- **Operational surface.** Standardize config, logging, metrics, health checks,
  storage migration reporting, sync lag, full-recompute counters, and protocol
  version mismatch diagnostics.
- **Server shell shape.** Define the smallest deployable wrapper around `Node`:
  typed config loading, storage opening/migration reporting, auth/session
  admission, WebSocket or transport listeners, health and metrics endpoints, and
  coordinated drain/shutdown. The shell may choose core, edge, or relay role
  configuration, but transaction, query, subscription, and sync semantics remain
  in their owning specs and are not re-exposed as server-only `Db` methods.
- **Failure behavior.** Specify reconnect, resume, backpressure, local queue
  limits, storage corruption reporting, auth expiry, and unsupported feature
  negotiation across every binding.
- **Packaging.** Publish reproducible browser, Node, and server artifacts with
  matching protocol/API versions and a compatibility policy.

### 17.4 P2 — polish and ecosystem fit

Milestone: **integrators can adopt jazz incrementally without bespoke glue.**

- **Framework adapters.** Provide thin React and server-framework adapters over
  the TypeScript API, without adding alternate semantics.
- **Hosted/serverless profile.** Document constraints for ephemeral compute,
  edge caches, durable core storage, and background compaction.
- **Migration playbooks.** Provide guides for schema lenses, branch-view-based
  previews, storage backend swaps, and protocol upgrades.
- **Observability recipes.** Ship dashboards or examples for sync health,
  subscription full-recompute budget, edge cache hit rate, and storage latency.
- **Compatibility gates.** Require release checks that compare API capabilities,
  protocol fixtures, storage contract fixtures, and conformance scenarios.

### 17.5 Milestone order

1. **Boundary sketch** — write the TS/WASM/NAPI direct-object binding shape,
   cross-binding capability matrix, row DTO fixtures, and wire envelope
   fixtures. The first representative scenario proofs are app-shaped memory DB
   flows, decoded row-record payloads, one subscription stream, and public
   `WireFrame` send/receive pumps.
2. **Local vertical slice** — run a TypeScript app through WASM or NAPI against
   a local server shell with transactions, reads, sync, and one subscription.
3. **Server shell slice** — run the local shell as a real executable/package:
   load config, open storage, admit one authenticated session, serve a
   WebSocket/byte-transport listener, publish health/metrics, and drain
   connections on shutdown while all product behavior still flows through the
   client API and shared sync protocol. The current `jazz-server` surface has
   two canaries: `cargo run -p jazz-cli --bin jazz-server -- dry-run`, which validates the
   default local shell plan without opening sockets; and
   `jazz_server::loopback_websocket::LoopbackWebSocketServer`, which sends
   postcard batches of raw ABI `WireFrame` bytes as binary WebSocket messages.
   HTTP schema routes are exercised through the production Axum router.
   The alpha TS/WASM gate now spawns the WebSocket listener as a Rust process
   and proves two-client todo convergence through that boundary.
4. **Storage slice** — prove the storage contract with durable and in-memory
   backends, including migration metadata.
5. **Subscription slice** — bridge maintained subscription deltas into stable
   TypeScript events and measure every full-diff full recompute.
6. **Lens/branch-view slice** — expose branch-view and lens facades across Rust,
   TypeScript, WASM, and NAPI with conformance tests.
7. **Topology slice** — run the same scenario suite across client, relay, edge,
   and core roles using the shared wire protocol.
8. **Release slice** — package artifacts, version compatibility checks, docs,
   and operational diagnostics for integrators.

### 17.6 Recorded packaging decision

- ✅ **Conformance storage backends:** the alpha matrix covers in-memory,
  IndexedDB, RocksDB, and SQLite through Groove's ordered-KV contract. Native mobile
  hosts use SQLite through a process-local native relay (chapter 19); that is a
  storage adapter and host boundary, not a new Jazz query or sync runtime.

## Open Questions

- 🔶 [#1778](https://github.com/garden-co/jazz/issues/1778) — Server, binding, framework, and operational integration contract.
- 🔶 [#1774](https://github.com/garden-co/jazz/issues/1774) — Serverless KV storage contract.
