# jazz — Specification · 17. Integrability roadmap

This chapter is the implementation roadmap for making jazz easy to embed from
TypeScript, WebAssembly, native Node, and server deployments while preserving the
normative contracts in chapters 2–16. It is an index of milestones and ownership:
operational details live in the chapters that own the semantics.

## 17.1 Target outcome

The target is one coherent product surface:

- browser apps call a TypeScript API backed by WASM;
- Node apps call the same TypeScript API backed by NAPI when available;
- servers run a small operational shell around `Node`, not a wider client `Db`;
- edge and core deployments are topology choices, not separate products;
- branches, lenses, subscriptions, and storage expose stable facades instead of
  leaking reference-implementation internals.

This roadmap does not redefine jazz semantics. When there is tension, the data
model, transaction, sync, topology, API, lowering, branch, lens, and maintained
subscription chapters win.

## 17.2 P0 — lock the integration contract

Milestone: **one runnable local app can use the public TypeScript API through
WASM or NAPI against a server shell without semantic forks.**

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
- **Lens/branch facades** — ch. 10 and ch. 11 own the schema-lens and branch
  lifecycle semantics; ch. 13 owns their product-facing API placement.

### 17.2.1 NAPI status and next practical step

There is no `jazz-napi` crate or Node package in the workspace yet. The future
native Node binding should live as a sibling to `jazz-wasm` (for example a
workspace `jazz-napi` crate, with package metadata beside it or inside it, once
packaging is chosen) because it is another host binding over the same Rust ABI,
not part of the core `jazz` crate and not a fork of the TypeScript harness.

The NAPI wrapper should follow the same shape as WASM: idiomatic host objects
around the real Rust `Db`, transactions, subscriptions, and transports. It must
reuse core payloads such as `ReadOpts`, `Error`, and `WireError`, and call
postcard directly where a byte payload is useful; it should not recreate a
command/event runtime inside Rust.

The smallest credible first NAPI milestone is a memory-only native package that
opens a `Db`, runs create/update/delete/query flows, exposes one subscription as
a host stream/callback, and proves the row-record decoder shape used by WASM
examples. Transport, worker ownership, durable storage, browser parity, and
package publishing should follow only after that direct object canary is green.

## 17.3 P1 — harden deployability

Milestone: **a browser client, Node client, edge node, and core node can run the
same conformance scenarios with topology-specific configuration only.**

- **Edge topology.** Implement deployment profiles for client, relay, edge, and
  core roles. Role flags decide fate authority, durability guarantees, caching,
  and eviction; protocol behavior stays shared.
- **Conformance matrix.** Add black-box tests that run the same API scenarios
  against Rust-only, WASM, NAPI, browser-worker, local server, and edge/core
  layouts. Cover mergeable/exclusive transactions, RLS, large values,
  subscription deltas, branches, and lenses.
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

## 17.4 P2 — polish and ecosystem fit

Milestone: **integrators can adopt jazz incrementally without bespoke glue.**

- **Framework adapters.** Provide thin React and server-framework adapters over
  the TypeScript API, without adding alternate semantics.
- **Hosted/serverless profile.** Document constraints for ephemeral compute,
  edge caches, durable core storage, and background compaction.
- **Migration playbooks.** Provide guides for schema lenses, branch-based
  previews, storage backend swaps, and protocol upgrades.
- **Observability recipes.** Ship dashboards or examples for sync health,
  subscription full-recompute budget, edge cache hit rate, and storage latency.
- **Compatibility gates.** Require release checks that compare API capabilities,
  protocol fixtures, storage contract fixtures, and conformance scenarios.

## 17.5 Milestone order

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
   three canaries: `cargo run -p jazz-server -- dry-run`, which validates the
   default local shell plan without opening sockets;
   `jazz_server::loopback_http::LoopbackHttpServer`, which starts a
   loopback-only HTTP bridge around `InMemoryServerShell` for health, metrics,
   session admission, and newline-separated hex frame request plumbing; and
   `jazz_server::loopback_websocket::LoopbackWebSocketServer`, which sends
   postcard batches of raw ABI `WireFrame` bytes as binary WebSocket messages.
   The alpha TS/WASM gate now spawns the WebSocket listener as a Rust process
   and proves two-client todo convergence through that boundary.
4. **Storage slice** — prove the storage contract with durable and in-memory
   backends, including large-value hooks and migration metadata.
5. **Subscription slice** — bridge maintained subscription deltas into stable
   TypeScript events and measure every full-diff full recompute.
6. **Lens/branch slice** — expose branch and lens facades across Rust,
   TypeScript, WASM, and NAPI with conformance tests.
7. **Topology slice** — run the same scenario suite across client, relay, edge,
   and core roles using the shared wire protocol.
8. **Release slice** — package artifacts, version compatibility checks, docs,
   and operational diagnostics for integrators.

## 17.6 Open questions

- 🔶 Which storage backends are required for the first conformance matrix:
  in-memory, browser persistent storage, RocksDB, SQLite, or all four?
- 🔶 What is the alpha replacement packaging line for the server shell: a
  dedicated crate/package with stable config types, a reference executable over
  unstable internals, or both?
- 🔶 Which auth/session admission inputs are mandatory for the first shell:
  bearer claims, signed `Hello` claims, injected test identity, or a pluggable
  verifier owned by ch. 7 and ch. 8?
- 🔶 Which operational endpoints are required for alpha replacement: liveness,
  readiness, storage migration state, sync lag, active session count, protocol
  version mismatch counters, and graceful-drain status?
- 🔶 How much listener policy belongs in config versus code: WebSocket paths,
  TLS termination assumptions, max frame/connection limits, backpressure
  thresholds, and allowed role/profile combinations?
- 🔶 Which TypeScript framework adapter, if any, should be the first blessed
  adapter after the core cross-binding capability gate passes?
