# jazz — Specification · Appendix A. Implementation discipline

## Overview

_Non-normative (guidance)._ This appendix records the engineering disciplines
that keep the implementation aligned with the specification. The `INV-DISC-*`
entries are audit anchors for code structure and tests, not
application-visible semantic law. Application semantics live in the numbered
chapters, and the SPEC, not the README, is the contract.

Invariant digest:

- `INV-DISC-1`: Node-core semantics MUST be deterministic under explicit driver inputs and remain directly simulatable.
- `INV-DISC-2`: Every cross-node semantic concept MUST have an exhaustive, serializable representation in the protocol, storage, or both.
- `INV-DISC-3`: Relay, edge, and core roles MUST share one node and message model rather than separate semantic implementations.
- `INV-DISC-4`: Commit, fate, and view ingestion MUST be idempotent and detect conflicting replays.
- `INV-DISC-5`: State with ordering or lattice semantics MUST use distinct types and monotone transitions.
- `INV-DISC-6`: Replicated state MUST remain structurally distinct from derived current state, which MUST be recomputed rather than replicated, and its provenance MUST remain attributable to its transaction.
- `INV-DISC-7`: Node correctness tests MUST use an independent oracle for randomized semantic checks.
- `INV-DISC-8`: Seeded tests MUST model reordering, duplication, restart, and rehydration hazards as explicit actions.
- `INV-DISC-9`: Parked work MUST be observable and MUST drain at quiescence.
- `INV-DISC-10`: Recovery MUST reconstruct semantic node state from durable storage rather than transport or session state.
- `INV-DISC-11`: Peer payload inventory and operational counters MUST remain implementation artifacts rather than semantic state.
- `INV-DISC-12`: Benchmark receipts MUST report deterministic work counters and timing ratios, and MUST NOT present dirty-worktree measurements as results.

## Details

### A.1 Simulation-first node core

The node core is designed to be simulated directly. Its behavior must be
deterministic, with no hidden dependence on transport, threads, clocks, or
randomness. Time enters only as an explicit `now_ms` parameter
(`TxTime::tick(register, now_ms)`, authority ingest's `now_ms`), and `Node` /
`PeerState` advance synchronously through explicit methods (`INV-DISC-1`).
Threading and channels belong only to integration drivers
(`threaded_four_tier`), never to node logic.

**Implementation status (verified).**
`m3_seeded_run_is_deterministic_for_fixed_seed` exercises fixed-seed replay.

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
`PublishSchema`, `PublishLens`, `SetCurrentWriteSchema`, `CatalogueAck`, and
`ViewUpdate`.

**Implementation status (verified).** `wire_fixture_messages` covers the
serializable `SyncMessage` fixtures.

### A.3 Roles, not separate implementations

Relay, edge, and core are roles over a shared node model, not separate semantic
implementations (`INV-DISC-3`, ch. 9). The same `Node` + `PeerState` machinery
serves all tiers: relay ingest stores pending units without assigning fate,
`PeerRole` controls link identity and read narrowing, and the four-tier tests
run every tier through the same types.

**Implementation status (verified).**
`four_tier_topology_relays_pending_units_and_core_fates` exercises the shared
topology.

### A.4 Idempotent, conflict-detecting ingestion

Ingestion must tolerate replay without hiding divergence. Commit, fate, and
view ingestion are idempotent and conflict-detecting (`INV-DISC-4`): a duplicate
unit with matching payload no-ops or returns the known fate, a conflicting
payload errors, and a stale `Pending` never regresses an `Accepted` fate.

**Implementation status (verified).**
`duplicate_commit_units_must_match_original_payload` and
`stale_pending_fate_update_cannot_regress_accepted` cover replay and fate
regression handling.

### A.5 Typed, monotone state

State that has ordering semantics must make those semantics visible in its
types. Time-like and lattice domains use distinct types with monotone
transitions (`INV-DISC-5`): `GlobalTime`, `TxTime`, `Fate`, and
`DurabilityTier` are separate newtypes; `GlobalTime::tick` explicitly packs a
46-bit physical millisecond component and 18-bit logical counter; and
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
catalogue/branch-column mappings, HLC/global-seq, pending edges, rejected headers) from
storage, never from transport/session state (`INV-DISC-10`).

**Implementation status (verified).**
`m3_seeded_sync_interleavings_converge_against_oracle` exercises the seeded
hazards, and the recovery suite covers reopening durable node state.

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

The canonical gate set is part of implementation discipline. Keep the gate list
in one SPEC location: appendix D mirrors the operational source of truth in
`.claude/CLAUDE.md`, including the ordinary full gate set, conditional benchmark
smoke, sensitive-data guard, and additional full-workspace gate for public
`jazz` type changes.

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
4. A source or test file growing beyond roughly 2,000 lines is a prompt to find
   a real concept boundary, not a mechanical file-splitting target.

## Open Questions

- 🔶 [#1786](https://github.com/garden-co/jazz/issues/1786) — Core discipline, seed conventions, type-aware linting, and tracing upkeep.
