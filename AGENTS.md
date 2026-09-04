# Jazz

Distributed, local-first relational database. Rust core, TypeScript client layers, WASM + NAPI + React Native bindings.

## Specs

Architecture docs live in `crates/jazz/SPEC/` and `crates/groove/SPEC/`
(chapters structured as Overview / Details / Open Questions); there is no
top-level `specs/` directory anymore. Private-side strategy/infra specs live
in the `jazz-private` repo.

## Durable encoding review

Whenever storage or wire formats are touched, explicitly review the change for
unspecified or default serializer encodings. Every authoritative encoding must
be named and versioned where appropriate, specified, and pinned by byte-level
corpus fixtures or receipts; do not treat a serializer's incidental layout as
a durable contract.

## Work style

### Durable follow-up and WIP visibility

GitHub Issues are the durable follow-up system. Before an orchestrator session
or lane is retired, capture or link every finding, decision, deferred task,
unresolved question, or adopter surprise that still needs follow-up in a GitHub
Issue. Specs and local executable manifests may link to those issues, but must
not duplicate a backlog. Do not issue-track ephemeral status or work that is
already complete.

An implementation lane reports its first coherent local commit immediately.
The coordinator then pushes it and opens or updates a clearly marked draft PR
as soon as work begins or that first commit exists; the PR may be red/WIP and
must not wait for completion or review. Lanes remain local-only: they must not
push, create or modify PRs, comment on GitHub, or merge.

### Pull-request descriptions

Every behavior-changing PR description explains the before/after behavior,
the governing invariants, and important non-goals or unchanged cases. Include
worked examples for the normal path and meaningful edge, failure, retry, or
handoff cases. For nuanced or large changes, make these concrete enough for an
adversarial reviewer to verify the behavior and for a future reader to recover
the decision without reconstructing it from the diff or conversation history.

### Stack and restack preflight

Before every `gh stack link`, `gh stack submit`, rebase, merge-based restack,
or branch propagation, run `dev/gates/require-clean-worktree.sh <checkout>`
against every checkout that the operation will mutate. It rejects staged index
changes, unstaged tracked changes, and non-ignored untracked files. Do not
interpret a clean `git diff` as sufficient: it deliberately does not report a
dirty index. Preserve or commit the state first; never let a stack operation
implicitly carry it across branches.

**Testing:** prefer black-boxed integration tests over unit tests or white-box tests.
Do not use JSON-like schema/permissions/query definitions. Always use the public API to build them in the tests.
Before writing any test in Rust crates, always read `crates/jazz/TESTING_GUIDELINES.md` in full and follow it.

**Builds:** `pnpm build:core` (all the packages), `pnpm test` (everything), via turbo.

**Focused Rust tests:** use `dev/t`, rather than a raw filtered `cargo test`.
It lists the selected `jazz` test target first, resolves one canonical Rust
module-path name, and runs that name with `--exact`; a miss or an ambiguous
filter is an error before any test run. For a library test, use
`dev/t unique::module::test_name`; for an integration target, use
`dev/t --test target_name unique::module::test_name`. The wrapper preserves the
core gate's `-p jazz --no-default-features --features testing,transport-compression-zstd` selection.

**Canonical gates:** do not let born-red or rotted targets accumulate silently.
For ordinary Rust/core work, the full gate set is:

**Local CI-equivalent gate.** `node dev/gates/local-ci-equivalent.mjs
--ci-equivalent` is the only local command that may be described as
_CI-equivalent_. It executes the exact named correctness/build command
partitions invoked by `.github/workflows/ci-suite.yml` (serialized locally;
CI schedules them in parallel). It fails closed on a missing partition and
includes the exhaustive workspace `--lib --bins --tests --examples --benches`
compile with CI's required features before TypeScript artifacts or suites.
The default `node dev/gates/local-ci-equivalent.mjs` is deliberately a faster
**focused** iteration mode and prints that it is **not CI-equivalent**. Lanes
must never call a focused, crate-only, or partial-artifact receipt
CI-equivalent. Use `--ci-partition <name>` only when reproducing one named CI
job during diagnosis; it is likewise not a full CI-equivalent result.

**Generated correctness bindings.** `pnpm build:correctness-artifacts` is the
native producer: it builds and seals the fast-WASM/release-NAPI pair into a
fingerprint-addressed read-only store under this
worktree's ignored `target/`, and writes a producer manifest bound to the exact
checkout SHA and artifact hashes. `pnpm test:typescript-consumers` is the
consumer: it rejects a missing, stale, or mismatched manifest before and after
building Jazz Tools and launching TS/browser suites. This protects against
accidental concurrent builds or workspace mutation, not a hostile same-UID
process: portable path-based WASM/NAPI consumers cannot provide that security
boundary. Do not copy or share generated
`pkg/`, NAPI generations, snapshots, or producer manifests between lanes;
rebuild in the checkout whose tests you are running. The boundary is deliberate
and fail-closed: native production can succeed even when a TypeScript consumer
build/test subsequently fails.

**Correctness-artifact cache boundary.** Native/WASM generations are producer
state, not Turbo cache entries. A NAPI generation can retain many GiB of Cargo
products, so Turbo's local/remote archives would both be unbounded and unsafe
to restore as correctness authority. Keep `jazz-napi#build`,
`jazz-wasm#build`, `jazz-wasm#build:fast`, and `jazz-tools#build` explicitly
`cache: false`; preserve the resolved-Turbo-graph assertion when changing this
boundary. Ordinary small package builds may use Turbo normally, but must never
add `.native-artifacts/**`, WASM `pkg/**`, or the correctness-artifact store as
cacheable outputs.

- `cargo test -p jazz`
- `cargo test -p groove`
- `cargo test -p jazz --no-default-features --features testing,transport-compression-zstd` (matches `crates/jazz/TESTING_GUIDELINES.md`).
- `cargo test -p jazz-cli --features test` covers the `jazz-tools` and
  `jazz-server` executable shells, including their process-level integration
  tests. The binary names are stable, but their Cargo package is `jazz-cli`;
  build them with `cargo build -p jazz-cli --bin jazz-tools` or
  `cargo build -p jazz-cli --bin jazz-server`.
- `cargo test -p jazz-otel` covers exporter/provider construction. Its ignored
  `sync_telemetry_otel` target is a manual receipt because it does not
  programmatically assert collector delivery.
- `cargo check -p jazz-sim --benches` on the realistic benchmark workflow
  (same-repository PRs bearing `benchmark`, non-bot default-branch pushes,
  manual runs, and nightly); it catches bench API rot without extending every
  ordinary PR's critical path.
- `dev/gates/ts-wire-codec.sh` for TypeScript/native-runtime wire-codec coverage
  (Anselm-approved 2026-07-07)
- `dev/gates/invariant-registry.sh` parses both record-per-invariant registries
  and fails on a malformed record, a duplicate id within one registry, a cited
  test that does not exist, or a `✓`-covered invariant citing no test. `now` +
  `untested` is reported but does not fail — that is documented debt the registry
  deliberately keeps visible. Add or amend only the relevant readable
  `SPEC/invariants/INV-*.md` record; the overview is navigation, not the
  authoritative merge surface.
- `node dev/gates/spec-open-questions.mjs` keeps every unresolved SPEC open
  question linked to a GitHub Issue while remaining fully offline.
- `node dev/gates/ignored-tests.mjs` validates the exact compiled Rust ignored
  inventory and all TypeScript quarantine markers directly from source. Every
  ignore annotation must state `#NNNN: reason`; there is no separate burndown
  manifest.
- `JAZZ_SEED_COUNT=300 cargo test -p jazz m3_maintained_one_shot_differential_oracle`
  for maintained-vs-one-shot equivalence coverage (Anselm-approved 2026-07-08)
- `cargo test -p jazz --test incremental_delivery_canary maintained_relation_include_single_row_changes_are_scale_independent -- --exact`
  enforces `INV-INC-1` for relation/include delivery.
- the sensitive-data guard (lives in `jazz-private/dev/gates/`, runs via the
  optional lefthook hook) to keep customer-specific fixture names, domains,
  and IDs out of the public repository.

Use a `-j` appropriate for the box. These gates previously specified `-j 2`,
which was a workaround for spurious `linking with cc failed` under parallel
builds on a memory-constrained laptop — a property of that machine, not of the
build. Cap it only if you actually observe linker OOM. For reference, a cold
`cargo test -p jazz -j 16` measured 2m23s wall / 18m18s CPU at ~2GB peak of
187GB, so memory was never the binding constraint there; on a small machine
`-j 2` is still the right answer.

Benchmark work has three deliberately separate gates:

- During local iteration, compile only the affected target with
  `dev/gates/benchmark-smoke.sh <jazz|jazz-sim> <bench>`. This is a debug
  `cargo check`, not `cargo bench`; it avoids release-wide RocksDB rebuilds and
  timing noise.
- Ordinary PR CI runs `dev/gates/benchmark-smoke.sh --ci`: deterministic core
  and jazz-sim scenario assertions. The realistic benchmark workflow runs
  `dev/gates/benchmark-smoke.sh --compile-ci` to compile every maintained
  benchmark API on same-repository benchmark-labeled PRs, non-bot
  default-branch pushes, manual runs, and nightly. Keep correctness assertions
  in tests, not in a timing receipt.
- CodSpeed currently compares the example benchmark crates only. Apply the
  `benchmark` label when that coverage is relevant; it refreshes nightly on the
  default branch. Native `jazz` and `jazz-sim` timing remains in the
  realistic benchmark workflow (same-repository benchmark-labeled PRs,
  non-bot default-branch pushes, manual runs, and nightly) until those suites
  are ported to CodSpeed. Do not run a repository-wide benchmark suite before
  push.

Any change to a public `jazz` type additionally gates the full workspace,
including examples.

This rule exists because previous misses stayed hidden too long: `four_tier`
was born-red for roughly nine commits; `large_blob_values_follow_ordinary_row_permissions`
was born-red at `e03780d70`; `jazz-server`'s `cli_dry_run` target rotted after a
core API evolution; and adding `SyncMessage::SubscribeRejected` broke jazz-sim
bench compilation two steps before the bench gate caught it.

Wide maintained-vs-one-shot soaks use
`JAZZ_SEED_COUNT=2000 cargo test -p jazz m3_maintained_one_shot_differential_oracle`
alongside the existing m3 soak conventions.

**Continuous simulation soak.** `.github/workflows/continuous-simulation-soak.yml`
runs the deterministic M3 sync-convergence and maintained-vs-one-shot oracle
nightly on the trusted `jazz-ci` runner, with individual seed receipts. Run the
same driver locally with `dev/gates/run-continuous-simulation-soak.sh --sync-seeds
2 --differential-seeds 2`; copy a failed case's replay command from
`target/simulation-soak/summary.json`. The nightly default is sync 100×200
commits and differential 50×20 steps at churn depths 10,1000. The 100000-depth
churn is deliberately deferred from nightly: it is available through
`--churn-depths 10,1000,100000` when a bounded weekly budget is established.

**Don't rewrite existing tests without permission.** Existing tests encode decisions about what correct behaviour looks like. If the task explicitly involves changing behaviour, updating the tests to match is the right thing to do. But if a test is failing simply because the implementation diverges from what the test expects, rewriting the test to match the new behaviour is risky — the test may well be correct and the implementation wrong. Treat that as a human-in-the-loop decision: surface it to the user rather than resolving it unilaterally.

**Gate cadence — batched (Anselm-approved 2026-07-11).** Levers may be _batched_
before a full canonical gate run: land several commits, then run the full gate
set once per batch before landing, rather than paying the full set per lever.
Per-lever, use focused checks (the affected suites + all three mechanism canaries)
and `/code-review` as the stopgap. Red checkpoint pushes are allowed when the
user explicitly requests them; report the known failures clearly. Two tiers make
this concrete:

- _Iteration tier_ (intra-batch, per lever): focused crate suites + the three
  incremental-delivery canaries + oracle at low seed count. For a benchmark,
  add the one-target `benchmark-smoke.sh` compile check. ~fast.
- _Landing tier_ (before merge): the full canonical set below + CI benchmark
  API/scenario smoke + the jazz-private sensitive-data guard. Performance work
  additionally needs the relevant CodSpeed or realistic-workflow receipt, not
  a local omnibus run.

**Sensitive-data guard.** the jazz-private sensitive-data guard (in lefthook pre-commit)
fails on customer-identifying strings. Real customer schemas/data live ONLY in
`jazz-private`; `jazz_core` uses anonymized, name-blind fixtures (perf/lowering
gates are name-blind, so fidelity is preserved). Never commit real schema, dumps,
PII, or non-anonymized fixtures to this public repo.

**Perf loop.** Iterate perf on the in-repo native harness (anonymized fixture,
`cargo bench` under `[profile.perf]`) — not the workspace/NAPI/artifact-copy route,
which is milestone-only end-to-end validation. Every perf receipt emits its own
phase breakdown (attribution-by-default). Lanes should end reports with a
one-line **tooling-friction** note: what setup would have saved wall-clock.
