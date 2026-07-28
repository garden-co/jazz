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

## Details

### D.1 Canonical gate source

`.claude/CLAUDE.md` is the operational source of truth for the repository's
canonical gates. This appendix mirrors that source for SPEC readers; if the two
diverge, update this appendix from `.claude/CLAUDE.md` rather than treating the
appendix as authoritative.

For ordinary Rust/core work, the full gate set is:

1. `cargo test -p jazz -j 2`
2. `cargo test -p groove -j 2`
3. `cargo test -p jazz-tools --features test -j 2`
4. `cargo test -p jazz-server -j 2`
5. `cargo check -p jazz-sim --benches`
6. `dev/gates/ts-wire-codec.sh`
7. `JAZZ_SEED_COUNT=300 cargo test -p jazz m3_maintained_one_shot_differential_oracle`
8. `cargo test -p jazz --test incremental_delivery_canary maintained_relation_include_single_row_changes_are_scale_independent -- --exact`
9. the sensitive-data guard from `jazz-private/dev/gates/`, normally reached
   through the optional lefthook hook

Run `dev/benchmarks/smoke.sh` for any change touching protocol, engine,
storage, or benchmark harnesses. A change to a public `jazz` type additionally
gates the full workspace, including examples.

### D.2 The tiers

- **Crate tests** — integration and crate tests for `jazz`, `groove`,
  `jazz-tools` with its `test` feature, and `jazz-server`.
- **Bench API compilation** — `cargo check -p jazz-sim --benches` is always in
  the ordinary gate set because benchmark API rot has previously hidden until
  late in a lane.
- **TS/native wire codec** — `dev/gates/ts-wire-codec.sh` is the current
  TypeScript/native-runtime wire-codec gate. `dev/gates/` currently contains
  this gate and no legacy JS ABI decoder or WASM binding script.
- **Maintained-vs-one-shot oracle** —
  `JAZZ_SEED_COUNT=300 cargo test -p jazz m3_maintained_one_shot_differential_oracle`
  is the canonical randomized equivalence gate; `JAZZ_SEED_COUNT=2000` is the
  wide soak form.
- **Incremental delivery canary** —
  `cargo test -p jazz --test incremental_delivery_canary maintained_relation_include_single_row_changes_are_scale_independent -- --exact`
  enforces `groove/SPEC/INVARIANTS.md::INV-INC-1` for relation/include delivery.
- **Sensitive-data guard** — the guard in `jazz-private/dev/gates/` keeps
  customer-specific fixture names, domains, and ids out of the public
  repository.
- **Benchmark smoke** — `dev/benchmarks/smoke.sh` is conditional on changes to
  protocol, engine, storage, or benchmark harnesses.
- **Public type changes** — changes to public `jazz` types additionally gate the
  full workspace, including examples.
- **Server shell** — `cargo test -p jazz-server` exercises the in-memory Rust
  server shell over the public frame pump, including subscriber accept, detach
  for resume, resume-token rejection, drain/health transitions, and metrics. It
  also starts the loopback HTTP byte-frame listener on `127.0.0.1:0` and covers
  health, metrics, session creation, and newline-separated hex frame request
  plumbing into `InMemoryServerShell`. The loopback WebSocket listener is also
  covered with real ABI clients: each binary message is a postcard batch of raw
  `WireFrame` bytes, and the test proves writer-to-reader sync through the
  socket boundary.

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

### D.5 Current CI gap

The required local gates and the GitHub Actions workflow are not equivalent yet.
The canonical set above is the pre-push discipline mirrored from
`.claude/CLAUDE.md`; this distinction remains explicit as required by
`INV-TEST-4`.

## Open Questions

### Open questions

- 🔶 **CI scope.** Decide which canonical local gates should become GitHub
  Actions gates.
- 🔶 **Test catalogue ownership.** The old test-catalogue inventory is folded
  here: keep tests organized by public contract owner, not by historical module,
  and prefer black-box integration coverage for Rust crate behavior.
- 🔶 **Multi-server topology tests.** Add integration tests that exercise client
  to edge to core communication, including reconnect, policy narrowing,
  subscription deltas, and durability waits.
- 🔶 **Browser storage fallback tests.** OPFS-unavailable modes need explicit
  browser coverage for fail-loud or in-memory fallback behavior.
- 🔶 **WASM teardown regression.** Keep navigation/teardown churn coverage for
  multi-client WASM transports until the true shutdown fix lands.
