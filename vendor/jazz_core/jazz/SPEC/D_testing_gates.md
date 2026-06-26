# jazz — Specification · Appendix D. Testing & gates

*Non-normative (guidance).* This appendix defines the verification tiers, the
local gate stack, and the simulation-first testing discipline used to keep the
system reproducible under review. `INV-TEST-*` entries are process anchors.
Benchmark scenario detail lives in appendix B; this appendix links to that
detail rather than duplicating it.

## D.1 The local gate stack

The local gate stack is ordered so cheap, broad failures surface before more
specialized simulation checks. From the repository root, run:

1. `cargo fmt --all --check`
2. `cargo clippy --workspace --all-targets -- -D warnings`
3. `cargo test -p groove` · `cargo test -p jazz` ·
   `cargo test -p jazz-server`
4. `cargo test --doc -p groove` · `cargo test --doc -p jazz` (when public
   examples/docs change)
5. `node jazz/fixtures/js/decode_abi_fixtures.js`
6. `scripts/test_wasm_bindings.sh` when binding ABI/example code changes
7. `cargo test -p jazz-sim --test scenario_smoke`
8. `JAZZ_SEED_COUNT=<n> cargo test -p jazz m3_seeded_sync` when widening the
   seeded sync sweep beyond the fixed default crate-test coverage

## D.2 The tiers

- **Crate tests** — unit tests and `tests/` integration tests for `groove` and
  `jazz`.
- **Doctests** — public examples and API documentation, gated when those
  surfaces change.
- **Scenario smoke** — the scenario benchmark modules compile as tests and run
  small correctness profiles cheaply: `s1_saas_smoke`,
  `s1_saas_db_surface_smoke`,
  `s2_canvas_smoke`, `s3_permissions_smoke`,
  `s4_order_processing_smoke_debug_profile`, `s5_durable_stream_smoke`,
  `s6_text_traces_smoke`, `s7_migrations_smoke`, `s9_durable_execution_smoke`
  (no S8 — no harness yet). Binding gates should be rebuilt around direct
  WASM/NAPI-style object wrappers plus decoded row-record payloads.
- **Binding payload surface** — focused tests should prove core payload round
  trips for row batches, encoded patches, subscription events, write state, and
  structured errors. Direct binding behavior belongs in WASM/NAPI wrapper tests,
  not in a Rust command runtime.
  that drives `Db::tick` and decodes emitted wire frames, plus a memory storage
  snapshot import/export round trip that reopens readable rows.
- **Seeded deterministic sweep** — `m3_seeded_sync_interleavings_converge_against_oracle`
  drives seeded duplication, reordering, and redelivery, then asserts
  convergence against the oracle. `JAZZ_SEED=<u64>` forces a single replay,
  `JAZZ_SEED_COUNT` widens the sweep, and `JAZZ_COMMIT_COUNT` deepens each seed
  (default 24). A separate
  `m3_seeded_run_is_deterministic_for_fixed_seed` proves bit-for-bit replay
  (`INV-TEST-1`), while
  `lens_parallel_materialization_oracle_matches_engine_reads_seeded` serves as
  the schema/lens seeded oracle gate.
- **DTO/wire fixture gates** — future fixture tests should prove checked-in
  postcard canaries still decode to expected protocol and binding DTO shapes.
  payload fixtures should be generated directly from core APIs and cover future
  row-record binding payloads plus wire `WireFrame` envelopes.
- **TS/WASM binding harness** — `scripts/test_wasm_bindings.sh` should be rebuilt
  around direct object bindings: the alpha-shaped Node todo gate, browser-worker
  package, and WASM/NAPI wrappers over real `Db`/subscription/transport objects.
  The harness should rebuild `jazz-wasm` for Node and web targets, typecheck the
  TypeScript examples, pump an alpha-style local-first todo flow through byte transport,
  exercise alpha todo bool equality and title `contains` reads, cover
  shared-with-me access through `todo_shares`, verify deterministic
  identity-scoped policy reads through `dbReadForIdentity`, assert
  identity-scoped update dry-runs through `dbCanUpdateEncodedForIdentity`,
  exercise the alpha in-process server facade plus HTTP/SSE server gate
  including snapshot-backed restart, spawn a Rust `jazz-server` loopback
  WebSocket listener and prove two-client todo convergence plus durable todo/chat
  restart through binary `WireFrame` messages, bundle the browser scaffold, and
  run headless Chromium smokes against the Web Worker-backed scenario and
  snapshot-backed reload
  persistence. Use `scripts/test_wasm_bindings.sh --install` on a fresh
  checkout to run `npm ci` in each example package before the gates.
- **Server shell** — `cargo test -p jazz-server` exercises the in-memory Rust
  server shell over the public frame pump, including subscriber accept, detach
  for resume, resume-token rejection, drain/health transitions, and metrics. It
  also starts the loopback HTTP byte-frame listener on `127.0.0.1:0` and covers
  health, metrics, session creation, and newline-separated hex frame request
  plumbing into `InMemoryServerShell`. The loopback WebSocket listener is also
  covered with real ABI clients: each binary message is a postcard batch of raw
  `WireFrame` bytes, and the test proves writer-to-reader sync through the
  socket boundary.

## D.3 Simulation-first discipline

Deterministic simulation is a design constraint, not a test afterthought. The
node core is modeled as a pure state machine over explicit events; time,
randomness, and delivery order enter through drivers (appendix A). That boundary
is what makes failures replayable and reviewable.

The review rule forbids `Instant::now()`, `SystemTime`, `rand`, and thread
spawns inside node logic. A failure to replay bit-for-bit is itself a bug
(`INV-TEST-2`). The three driver modes provide complementary evidence:
**deterministic** runs use a stable order, **fuzz** runs inject seeded
duplication/reordering/redelivery, and **threaded** runs exercise load realism.
Wide soaks run `--release` (e.g. `JAZZ_SEED_COUNT=1000 cargo test --release -p
jazz m3_seeded_sync`).

## D.4 Oracle norm and public-surface preference

Every consistency claim gets randomized oracle coverage. The coverage includes
domination, merge convergence, exclusive validation, and sync convergence
(`INV-TEST-3`).

Tests prefer the public surfaces: the jazz `Db` facade and groove `Database`.
The SaaS `Db` smoke test is the model: subscribe via `db.subscribe`, mutate
through `insert_with_id`/`update`, wait on `DurabilityTier::Local`, and compare
query/subscription results against a local oracle. Internal hooks are reserved for
behavior that cannot be observed through the public surface, or for narrow
lower-level tests that best pin an invariant.

## D.5 Current CI gap

The required local gates and the GitHub Actions workflow are not equivalent yet.
GitHub Actions runs rustfmt, groove clippy/tests/bench-smoke, jazz crate tests
(`cargo test -p jazz`), jazz-server crate tests (`cargo test -p jazz-server`),
the JS ABI fixture decoder canary, and the jazz scenario smoke tier (`cargo test
-p jazz-sim --test scenario_smoke`). Workspace clippy, conditional doctests, and
the TS/WASM binding harness are still local pre-merge discipline
(`INV-TEST-4` would require closing that gap fully). The default jazz crate test
run includes the fixed-seed sync sweep.

## Open questions

- 🔶 **CI scope.** Should CI run workspace clippy, or keep it as pre-merge
  discipline?
- 🔶 **Fixed-seed count.** Earlier docs said seven fixed M3 seeds; the code
  defines eight. Reconcile, and pin the canonical count in D.2.
