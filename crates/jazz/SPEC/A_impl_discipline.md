# jazz — Specification · Appendix A. Implementation discipline

## Overview

_Non-normative (guidance)._ This appendix records the engineering disciplines
that keep the implementation aligned with the specification. The `INV-DISC-*`
entries are audit anchors for code structure and tests, not
application-visible semantic law. Application semantics live in the numbered
chapters, and the SPEC, not the README, is the contract.

Invariant digest:

- `INV-DISC-1`: node-core work must remain simulation-first. Load-bearing rule: production node semantics are exercised through deterministic inputs and explicit method/event surfaces...
- `INV-DISC-2`: all cross-node sync must use exhaustive serializable message enums, and every concept must be reachable through protocol messages, node storage, or both. Current SyncM...
- `INV-DISC-3`: relays and peers are roles over the same node/message vocabulary, not separate semantic implementations. Node::ingestrelaycommitunit stores pending local units without...
- `INV-DISC-4`: commit/fate/view ingestion must be idempotent and conflict-detecting. Duplicate relay commit units compare transaction payload and canonical versions, then no-op if id...
- `INV-DISC-5`: time-like and state-lattice domains must use distinct types and monotone transitions. Identifiers: GlobalSeq, TxTime, Fate, DurabilityTier. GlobalSeq::next is explicit...
- `INV-DISC-6`: the implementation must preserve structural column taxonomy. Wire payloads carry VersionRecord data and not local/global-derived currentness (protocol.rs lines 110-140...
- `INV-DISC-7`: oracle-first testing is part of implementation discipline. The oracle is independent of groove (oracle.rs lines 1-24); seeded M3 runs compare core/global/current/subsc...
- `INV-DISC-8`: out-of-order, duplicate, restart, and rehydrate hazards must be first-class seeded-test actions. The M3 harness duplicates upstream/fate/view messages, delivers child...
- `INV-DISC-9`: parked work must be observable and drained at quiescence. SyncMetrics currently tracks parkedorphans, parkedorphansresolved, parkedincomplete, parkedincompleteresolved...
- `INV-DISC-10`: crash/restart recovery must rebuild in-memory node discipline state from storage, not from transport/session state. recoverfromstorage rebuilds aliases, schema aliases...
- `INV-DISC-11`: peer-level complete-tx payload inventory and deterministic counters are implementation artifacts, not semantic state. PeerState owns shippedcompletetxpayloads, per-sub...
- `INV-DISC-12`: benchmarks are discipline gates that report deterministic counters plus timing ratios, but appendix A should not quote dirty-tree numbers. Sync benchmark emits JSON fi...

## Details

### A.1 Simulation-first node core

The node core is designed to be simulated directly. Its behavior must be
deterministic, with no hidden dependence on transport, threads, clocks, or
randomness. Time enters only as an explicit `now_ms` parameter
(`TxTime::tick(register, now_ms)`, authority ingest's `now_ms`), and `Node` /
`PeerState` advance synchronously through explicit methods (`INV-DISC-1`).
Threading and channels belong only to integration drivers
(`threaded_four_tier`), never to node logic.

### A.2 Everything reachable through messages or storage

Every cross-node concept must have an explicit place in the protocol, storage,
or both. The implementation style that supports that rule is deliberately
plain: structs, exhaustive enums, and handwritten match arms in the style of
groove's `OpType`; no trait-object hierarchies, no actor frameworks, and no
abstraction before a second concrete use exists. Cross-node concepts travel
through exhaustive, serializable message enums with a wire version field from
day one (ch. 8), and every concept is reachable through a protocol message,
node storage, or both (`INV-DISC-2`). The `SyncMessage` set is `CommitUnit`,
`FateUpdate`, `RegisterShape`, `Subscribe`, `SubscribeRejected`, `Unsubscribe`,
`PublishSchema`, `PublishLens`, `SetCurrentWriteSchema`, `CatalogueAck`,
`ViewUpdate`, `FetchContentExtent`, and `ContentExtents`.

### A.3 Roles, not separate implementations

Relay, edge, and core are roles over a shared node model, not separate semantic
implementations (`INV-DISC-3`, ch. 9). The same `Node` + `PeerState` machinery
serves all tiers: relay ingest stores pending units without assigning fate,
`PeerRole` controls link identity and read narrowing, and the four-tier tests
run every tier through the same types.

### A.4 Idempotent, conflict-detecting ingestion

Ingestion must tolerate replay without hiding divergence. Commit, fate, and
view ingestion are idempotent and conflict-detecting (`INV-DISC-4`): a duplicate
unit with matching payload no-ops or returns the known fate, a conflicting
payload errors, and a stale `Pending` never regresses an `Accepted` fate.

### A.5 Typed, monotone state

State that has ordering semantics must make those semantics visible in its
types. Time-like and lattice domains use distinct types with monotone
transitions (`INV-DISC-5`): `GlobalSeq`, `TxTime`, `Fate`, and
`DurabilityTier` are separate newtypes; `GlobalSeq::next` is explicit; and
backward or conflicting transitions surface as `NonMonotoneState` /
`ConflictingFate`. The column taxonomy from ch. 2 is preserved structurally:
wire payloads carry only replicated-immutable data, derived currentness is
recomputed, and upstream state lives on the transaction record (`INV-DISC-6`).

### A.6 Oracle-first testing and seeded hazards

Correctness tests are anchored by an independent truth model. The brute-force
`Oracle` is complete-history and groove-independent, and tests compare behavior
against it (`INV-DISC-7`). Distribution hazards are _first-class seeded-test
actions_, not afterthoughts: the M3 harness duplicates upstream/fate/view
messages, delivers children before parents, restarts readers and core, emits
rehydrates, and asserts quiescent drains (`INV-DISC-8`). Parked work is
observable via `SyncMetrics` (`parked_orphans`, `…_resolved`,
catalogue/incomplete variants) and must drain at quiescence (`INV-DISC-9`);
relatedly, a snapshot read asserts the relevant pending queues are drained
first. **Harness action caps are assertions too:** a cap on restarts,
rehydrates, parking, or drain work defines how much of that hazard class a seed
exercises. Initializing a counter _at_ its cap silently disables coverage and is
a test bug, not a valid simplification. Recovery rebuilds node state (aliases,
catalogue/branch metadata, HLC/global-seq, pending edges, rejected headers) from
storage, never from transport/session state (`INV-DISC-10`).

### A.7 Counters and benchmarks as gates

Operational counters are gates for discipline, not part of application
semantics. Per-peer complete-tx payload inventory and deterministic counters
(`PeerState.shipped_complete_tx_payloads`, `PeerMetrics`, `SyncMetrics`) are
implementation artifacts, and tests assert them; for example,
`version_bundles_out == shipped_complete_tx_payloads().len()` plus duplicate complete
payload bundles per link (`INV-DISC-11`). The sync and validation benchmarks
report deterministic counters plus timing ratios as discipline gates
(`INV-DISC-12`, appendix B), and they should never quote dirty-tree numbers as
results. Metrics are _not_ one unified struct: they are split across
`SyncMetrics`, `PeerMetrics`, and benchmark-computed values.

### A.8 Host-shell wiring canaries

Harness topologies are necessary but not sufficient for role semantics. The
edge-fate authority bug showed why: hand-wired four-tier tests exercised the
correct edge ingest path, while the production server shell routed the same
client upload through the core authority path. Convention: for every
host-shell role x ingest/dispatch path combination, at least one black-box test
must flow through the production shell and assert the semantics that role must
produce, including a paired discriminator when another role intentionally keeps
different behavior.

### A.9 Canonical gates

The canonical Rust gate set is part of implementation discipline. A branch that
changes Rust/core behavior should be able to pass:

- `cargo test -p jazz -j 2`
- `cargo test -p groove -j 2`
- `cargo test -p jazz-tools --features test -j 2` (the public API gate named in
  `crates/jazz-tools/TESTING_GUIDELINES.md`)
- `cargo test -p jazz-server -j 2`
- `cargo check -p jazz-sim --benches`

`cargo check -p jazz-sim --benches` is always in the set because it is cheap and
catches public enum/API drift in benchmarks before smoke or release work. Run
`dev/benchmarks/smoke.sh` for any change touching protocol, engine, storage, or
benchmark harnesses. A change to a public `jazz` type additionally gates the
full workspace, including examples, because public type changes can break
downstream crates without changing core tests.

This discipline was added after four concrete misses:

- `four_tier_topology_relays_pending_units_and_core_fates` rode born-red for
  roughly nine commits.
- `large_blob_values_follow_ordinary_row_permissions` was born-red at
  `e03780d70`.
- `jazz-server`'s `cli_dry_run` target rotted after a core API evolution.
- Adding `SyncMessage::SubscribeRejected` broke jazz-sim bench compilation and
  was caught two steps late.

### A.10 Structural discipline

Structure should make the design easy to audit. Large implementation concepts
should be immediately findable, algorithms should read as large steps before
small ones, and parallel representations or forwarding wrappers should be
collapsed when they no longer carry independent semantics. Completed structural
slices live in git history, not in this appendix.

Remaining jazz moves:

- **`query_eval.rs` split.** The file is still about 2.9k lines. Split
  registration/lifecycle, lowering, and evaluation into separate homes; move the
  semantic oracle into the testing-gated oracle module.
- **`ingest.rs` grouping.** Keep the parking family together as one block, and
  keep exclusive-predicate validation together as one block or move it to
  `validation.rs`.

Durable style rules:

1. File heads carry the concept: entry points and the large-step narrative come
   first; helpers follow; `mod` docs say what lives here and what deliberately
   does not.
2. One representation per truth; where two structures share a shape for
   different roles, the names must carry the roles.
3. No wrapper without semantics: forwarding-only types and value round-trips are
   debt by definition.

## Open Questions

### Open questions

- 🔶 **Canonical step API.** Should the discipline require the literal `step(&mut
NodeState, Event) -> Vec<OutboxMessage>` dispatcher (`Event`/`OutboxMessage`
  exist) or keep the weaker "reducible to deterministic events/methods" rule
  implemented by direct deterministic methods?
- 🔶 **Seed env vars.** Tests use `JAZZ_SEED` / `JAZZ_SEED_COUNT` /
  `JAZZ_COMMIT_COUNT`; the benchmarks still use `GROOVE_*`. Reconcile the naming.
- 🔶 **Discipline invariant tests.** `INV-DISC-*` already have registry rows,
  marked as guidance anchors (not conformance) in the registry header; decide
  whether any should also get enforcing tests.
- 🔶 **Type-aware lint lane.** The old oxlint TODO moves here: decide whether
  type-aware linting becomes a required local gate, a CI-only package lane, or a
  package-maintainer tool outside the crate contract.
- 🔶 **WASM tracing upstreaming.** Track local wasm-tracing improvements that
  should be contributed upstream or replaced by upstream releases, so debugging
  hooks do not remain private forks.
