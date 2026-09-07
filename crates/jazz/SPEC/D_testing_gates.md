# jazz — Specification · Appendix D. Testing & gates

## Overview

_Non-normative (guidance)._ This appendix defines the verification tiers, the
local gate stack, and the simulation-first testing discipline used to keep the
system reproducible under review. `INV-TEST-*` entries are process anchors.
Benchmark scenario detail lives in appendix B; this appendix links to that
detail rather than duplicating it.

Invariant digest:

- `INV-TEST-1`: Fixed-seed simulation and schema/lens materialization checks MUST replay deterministically.
- `INV-TEST-2`: Node-core simulation MUST be deterministic under driver-supplied time, randomness, and delivery order.
- `INV-TEST-3`: Every consistency claim MUST have randomized oracle coverage.
- `INV-TEST-4`: The canonical local gate set MUST be maintained as the pre-push verification contract, and any difference from CI MUST be explicit.
- `INV-TEST-5`: Structured-result maintained-vs-one-shot equivalence MUST use canonical `ResultTree` equality, not root-membership set equality.
- `INV-TEST-6`: Canonical-sync reconstruction coverage MUST prove that a receiver derives its terminal from a manifest-verified authored-schema witness closure rather than a replicated projected row.

## Details

### D.1 Canonical gate source

`.claude/CLAUDE.md` is the operational source of truth for the repository's
canonical gates. This appendix mirrors that source for SPEC readers; if the two
diverge, update this appendix from `.claude/CLAUDE.md` rather than treating the
appendix as authoritative.

For ordinary Rust/core work, the full gate set is:

1. `cargo test -p jazz`
2. `cargo test -p groove`
3. `cargo test -p jazz --no-default-features --features testing,transport-compression-zstd`
4. `cargo check -p jazz-sim --benches` on the realistic benchmark workflow
5. `dev/gates/ts-wire-codec.sh`
6. `JAZZ_SEED_COUNT=300 cargo test -p jazz m3_maintained_one_shot_differential_oracle`
7. `cargo test -p jazz --test incremental_delivery_canary maintained_relation_include_single_row_changes_are_scale_independent -- --exact`
8. the sensitive-data guard from `jazz-private/dev/gates/`, normally reached
   through the optional lefthook hook

For a benchmark edit, locally run
`dev/gates/benchmark-smoke.sh <jazz|jazz-sim> <bench>`; it is a targeted debug
compile check. The standalone `dev/gates/benchmark-smoke.sh --ci` executes the
deterministic core and jazz-sim scenario assertions for callers that need it.
The ordinary PR workspace partition runs those same named cases in its broad
Nextest selection with CI's feature set, first failing closed if the selected
inventory omits either smoke binary or any named scenario case. The realistic
benchmark workflow runs `dev/gates/benchmark-smoke.sh --compile-ci` to check
all maintained benchmark APIs on same-repository benchmark-labeled PRs,
non-bot default-branch pushes, manual runs, and nightly. CodSpeed evaluates the
example benchmark crates on benchmark-labeled PRs and nightly; native `jazz`
and `jazz-sim` timing remains in the realistic benchmark workflow until it is
ported. No local omnibus benchmark script is a push gate. A change to a public
`jazz` type additionally gates the full workspace, including examples.

Use a `-j` appropriate for the box; see PR #1157 for the rationale behind
replacing the former fixed `-j 2` guidance.

### D.2 The tiers

- **Crate tests** — integration and crate tests for `jazz` and `groove`.
- **Bench API compilation** — `cargo check -p jazz-sim --benches` runs on the
  realistic benchmark workflow (same-repository benchmark-labeled PRs,
  non-bot default-branch pushes, manual runs, and nightly), where it catches
  benchmark API rot without extending every ordinary PR's critical path.
- **TS/native wire codec** — `dev/gates/ts-wire-codec.sh` is the current
  TypeScript/native-runtime wire-codec gate. `dev/gates/` currently contains
  this gate and no legacy JS ABI decoder or WASM binding script.
- **Maintained-vs-one-shot oracle** —
  `JAZZ_SEED_COUNT=300 cargo test -p jazz m3_maintained_one_shot_differential_oracle`
  is the canonical randomized equivalence gate; `JAZZ_SEED_COUNT=2000` is the
  wide soak form. The test is currently Rust-ignored because canonical seed 47
  fails at fuzz-step-1 and seed 4,372,288 at fuzz-step-2; both are tracked in
  source ignore annotation. Replay either bounded failure with `JAZZ_SEED=<seed>
  JAZZ_DIFFERENTIAL_CHURN_DEPTHS=10,1000 JAZZ_DIFFERENTIAL_STEP_COUNT=3` and
  the fully qualified command below with `--exact --ignored`. CI compiles only
  the `--lib` test binary before separately bounding its semantic execution,
  then executes the bounded, real seed-11 smoke
  `JAZZ_SEED=11 JAZZ_DIFFERENTIAL_CHURN_DEPTHS=10,1000 JAZZ_DIFFERENTIAL_STEP_COUNT=3
cargo test -p jazz --lib node::tests::harness::m3_maintained_one_shot_differential_oracle -- --exact --ignored`.
  That smoke preserves the oracle assertions but is not a substitute for the
  quarantined multi-seed gate.
- **Incremental delivery canary** —
  `cargo test -p jazz --test incremental_delivery_canary maintained_relation_include_single_row_changes_are_scale_independent -- --exact`
  enforces `groove/SPEC/INVARIANTS.md::INV-INC-1` for relation/include delivery.
- **Sensitive-data guard** — the guard in `jazz-private/dev/gates/` keeps
  customer-specific fixture names, domains, and ids out of the public
  repository.
- **Benchmark API and scenario smoke** — Ordinary CI runs deterministic
  scenario assertions. The realistic benchmark workflow compiles all
  maintained benchmark targets; CodSpeed compares example benchmark crates;
  native `jazz` and `jazz-sim` timing stays in the realistic workflow until
  migrated.
- **Public type changes** — changes to public `jazz` types additionally gate the
  full workspace, including examples.
- **Server shell** — the server-shell tests are included in the `jazz` package
  gates above. They exercise the in-memory Rust server shell over the public
  frame pump, production Axum HTTP routes, loopback WebSocket listeners, and
  real ABI clients.

### D.3 Simulation-first discipline

Deterministic simulation is a design constraint, not a test afterthought. The
node core is modeled as a pure state machine over explicit events; time,
randomness, and delivery order enter through drivers (appendix A). That boundary
is what makes failures replayable and reviewable.

The review rule forbids `Instant::now()`, `SystemTime`, `rand`, and thread
spawns inside node logic. A failure to replay bit-for-bit is itself a bug
(`INV-TEST-2`). The three driver modes provide complementary evidence:
**deterministic** runs use a stable order, **fuzz** runs inject seeded
duplication/reordering/redelivery, and **threaded** runs exercise load realism.
Wide soaks use the maintained-vs-one-shot oracle form named above, alongside the
existing M3 soak conventions.

**Implementation status (verified).**
`m3_seeded_run_is_deterministic_for_fixed_seed` and
`m3_maintained_one_shot_differential_oracle` exercise deterministic replay and
randomized maintained-vs-one-shot equivalence.

### D.4 Oracle norm and public-surface preference

Every consistency claim gets randomized oracle coverage. The coverage includes
domination, merge convergence, exclusive validation, and sync convergence
(`INV-TEST-3`).

**Implementation status (verified).**
`m3_seeded_sync_interleavings_converge_against_oracle` is the seeded sync
oracle test.

Tests prefer the public surfaces: the jazz `Db` facade and groove `Database`.
The SaaS `Db` smoke test is the model: subscribe via `db.subscribe`, mutate
through `insert_with_id`/`update`, wait on `DurabilityTier::Local`, and compare
query/subscription results against a local oracle. Internal hooks are reserved for
behavior that cannot be observed through the public surface, or for narrow
lower-level tests that best pin an invariant.

### D.4.1 Structured-result oracle boundary

**Current coverage limit (verified 2026-08-04).**
`m3_maintained_one_shot_differential_oracle` compares only
`BTreeSet<(table, RowUuid)>` (`crates/jazz/src/node/tests/m3_differential.rs:896-936`).
It discards cells, content versions, relation edges, position/order, and
duplicates. It therefore proves root-membership convergence only; it does not
prove output content, ordering, nested association, or delivery shape. The
current differential also reduces in-process updates rather than exercising
receiver application or chunk assembly.

**Target contract.** Before extending that oracle for structured output, define
a canonical public-facing `ResultTree` reducer and equality relation. Equality
MUST include the ordered root sequence; each node's output identity and selected
values; named child relations and each relation's ordered child sequence;
recursive descendants; aggregate/group payloads; and the specified semantic
states for null, hole, and empty relation. Identity is part of the comparison;
it MUST NOT replace payload equality. Transport provenance/coverage facts that
are contractual remain separately testable protocol facts rather than implicit
tree fields.

The future oracle MUST reduce maintained snapshots and whole-parent deltas at a
receiver-facing result boundary, then compare that tree with a one-shot tree at
the same frontier. It should additionally cover valid chunk assembly (no
publication before the final chunk) and exact order at every relation boundary.
This is target coverage; no test currently enforces `INV-TEST-5`.

### D.4.2 Canonical-sync reconstruction test ladder

The target test ladder for ch. 8 §8.4.1 is intentionally staged. Each rung must
prove local derivation from canonical input closure; asserting that a receiver
was handed the expected projected row is not evidence for this design.

| Rung                                         | Required black-box proof                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             | Current quarantine / migration mapping                                                                                                                                                                                    |
| -------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1. Authored fact codec and manifest identity | A `v1` version is rejected if its bytes are decoded as `v2`, accepted when decoded as `v1`, and produces the same canonical identity after receive/restart. A changed class inventory, digest, epoch, or residual-program id parks the closure.                                                                                                                                                                                                                                                                                                                                                      | New focused protocol/codec and manifest test; no existing quarantine isolates raw-byte relabeling.                                                                                                                        |
| 2. Full/reset install crash ladder           | Fault each point after staging every class member, after IVM precomputation, after durable manifest/fact/terminal/fast-receipt swap, after local-publication enqueue, and before local consumer observation. Recovery exposes only the old or new complete closure and never a partial terminal, settlement, fast receipt, or publication.                                                                                                                                                                                                                                                           | New focused durable-install/recovery tests.                                                                                                                                                                               |
| 3. Incremental manifest transition           | A successor names an exact active predecessor and per-class authenticated sparse-Merkle old/new roots/counts plus canonical add/remove proofs. Invalid membership/non-membership proof, predecessor, count, root, duplicate, or removal never changes the active closure and requests repair/reset. A valid transition changes only affected IVM inputs.                                                                                                                                                                                                                                             | New focused transition tests, including a planted full-rebuild detector for `INV-INC-1` and an independently recomputed proof/root oracle.                                                                                |
| 4. Simple root lineage                       | `v1.users { id, name, email }` received by a `v2.people { id, name, email_address }` subscriber selects the `v1` winner before projection and reaches the same selected root result as one-shot after ordered lens projection and restart, without a projected-row carrier or local-history supplement.                                                                                                                                                                                                                                                                                              | `multi_hop_column_additions_new_client_can_read_old_rows` and `multi_hop_column_renames_new_client_can_read_old_rows` are active regression coverage; `column_addition_new_client_can_read_old_rows` remains quarantined. |
| 5. Source identity, joins, and branch views  | Replacements/deletions retain authored branch-key identity through table rename and projected join lowering; the selected head and live/frozen base are isolated from unrelated branch keys and post-cut base rows; local IVM retracts/replaces the same output as the authority.                                                                                                                                                                                                                                                                                                                    | Target branch-view reconstruction coverage from ch. 11.                                                                                                                                                                   |
| 6. Opaque admission isolation                | Nested array/join updates and policy revocation converge from safe facts; unreadable evidence is never serialized. An opaque admission fact is rejected when replayed under any different authority lineage, authority epoch, shape/binding/read view, reader/policy revision, branch sources/SnapshotRef, residual-program identity, protected output/source occurrence, or concrete content/deletion/witness version/layer. In particular, a test swaps an allowed admission onto a denied row/output and applies a stale admission after its protected winner is replaced; both must fail closed. | Target branch-view reconstruction coverage from ch. 11.                                                                                                                                                                   |
| 7. Reconnect, repair, and reset              | After durable restart, eviction, a known-state declaration, and exact repair of each closure class, no terminal cache is needed to regain a settled local result; a missing catalogue/version/branch-source/correlation/admission/replacement/settlement fact blocks settlement, and an epoch mismatch resets.                                                                                                                                                                                                                                                                                       | `persisted_stale_edge_reconnect_replays_catalogue_before_client_work` is the catalogue-replay prerequisite; `edge_write_reaches_client_on_peer_edge` is the multi-edge delivery prerequisite.                             |
| 8. Aggregate boundary                        | Count/sum/min/max and ordered/windowed aggregates converge only from their complete admitted input multiset and deterministic witnesses; removing any input proves the terminal was locally recomputed. A privacy-preserving or otherwise non-reconstructible aggregate is rejected rather than carried as an authority summary.                                                                                                                                                                                                                                                                     | New focused maintained-subscription tests; existing aggregate terminal coverage is not reconstruction coverage.                                                                                                           |
| 9. Differential and fault injection          | Seeded multi-node runs vary manifest/epoch changes, lens arrival, duplicate/reordered closure facts, reconnect, policy changes, branch views, joins, arrays, and aggregate inputs; receiver `ResultTree` equals authority one-shot at each settled frontier.                                                                                                                                                                                                                                                                                                                                         | Extends `m3_maintained_one_shot_differential_oracle`; its present shared-source-only comparison is insufficient under §D.4.1.                                                                                             |

The named source ignore annotations above remain failures to be burned down, not
coverage claims. Rungs 1–8 should become focused deterministic tests first;
rung 9 becomes the property/oracle gate only after their failure diagnostics can
name the missing closure component.

### D.5 Current CI gap

The required local gates and the GitHub Actions workflow are not equivalent yet.
The canonical set above is the pre-push discipline mirrored from
`.claude/CLAUDE.md`; this distinction remains explicit as required by
`INV-TEST-4`.

## Open Questions

- 🔶 [#1787](https://github.com/garden-co/jazz/issues/1787) — Gate scope, test catalogue ownership, and topology/browser coverage.
