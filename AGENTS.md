# Jazz

Distributed, local-first relational database. Rust core, TypeScript client layers, WASM + NAPI + React Native bindings.

## Specs

Architecture docs live in `crates/jazz/SPEC/` and `crates/groove/SPEC/`
(chapters structured as Overview / Details / Open Questions); there is no
top-level `specs/` directory anymore. Private-side strategy/infra specs live
in the `jazz-private` repo.

## Work style

**Testing:** prefer black-boxed integration tests over unit tests or white-box tests.
Do not use JSON-like schema/permissions/query definitions. Always use the public API to build them in the tests.
Before writing any test in Rust crates, always read `crates/jazz-tools/TESTING_GUIDELINES.md` in full and follow it.

**Builds:** `pnpm build:core` (all the packages), `pnpm test` (everything), via turbo.

**Canonical gates:** do not let born-red or rotted targets accumulate silently.
For ordinary Rust/core work, the full gate set is:

- `cargo test -p jazz -j 2`
- `cargo test -p groove -j 2`
- `cargo test -p jazz-tools --features test -j 2` (matches `crates/jazz-tools/TESTING_GUIDELINES.md`)
- `cargo test -p jazz-server -j 2`
- `cargo check -p jazz-sim --benches` (always; it is cheap enough and catches bench API rot)
- `dev/gates/ts-wire-codec.sh` for TypeScript/native-runtime wire-codec coverage
  (Anselm-approved 2026-07-07)
- `JAZZ_SEED_COUNT=300 cargo test -p jazz m3_maintained_one_shot_differential_oracle`
  for maintained-vs-one-shot equivalence coverage (Anselm-approved 2026-07-08)
- `cargo test -p jazz --test incremental_delivery_canary maintained_relation_include_single_row_changes_are_scale_independent -- --exact`
  enforces `INV-INC-1` for relation/include delivery.
- `dev/gates/no-sensitive-data.sh` to keep customer-specific fixture names,
  domains, and IDs out of the public repository.

Run `dev/benchmarks/smoke.sh` for any change touching protocol, engine, storage,
or benchmark harnesses. Any change to a public `jazz` type additionally gates the
full workspace, including examples.

This rule exists because previous misses stayed hidden too long: `four_tier`
was born-red for roughly nine commits; `large_blob_values_follow_ordinary_row_permissions`
was born-red at `e03780d70`; `jazz-server`'s `cli_dry_run` target rotted after a
core API evolution; and adding `SyncMessage::SubscribeRejected` broke jazz-sim
bench compilation two steps before the bench gate caught it.

Wide maintained-vs-one-shot soaks use
`JAZZ_SEED_COUNT=2000 cargo test -p jazz m3_maintained_one_shot_differential_oracle`
alongside the existing m3 soak conventions.

**Don't rewrite existing tests without permission.** Existing tests encode decisions about what correct behaviour looks like. If the task explicitly involves changing behaviour, updating the tests to match is the right thing to do. But if a test is failing simply because the implementation diverges from what the test expects, rewriting the test to match the new behaviour is risky — the test may well be correct and the implementation wrong. Treat that as a human-in-the-loop decision: surface it to the user rather than resolving it unilaterally.

**Gate cadence — batched (Anselm-approved 2026-07-11).** Levers may be _batched_
before a full canonical gate run: land several commits, then run the full gate
set once per batch **before push**, rather than paying the full set per lever.
Per-lever, use focused checks (the affected suites + all three mechanism canaries)
and `/code-review` as the stopgap. Never push a batch that has not passed the full
set. Two tiers make this concrete:

- _Iteration tier_ (intra-batch, per lever): focused crate suites + the three
  incremental-delivery canaries + oracle at low seed count; skip smoke. ~fast.
- _Landing tier_ (before push): the full canonical set below + smoke +
  `dev/gates/no-sensitive-data.sh`.

**Sensitive-data guard.** `dev/gates/no-sensitive-data.sh` (in lefthook pre-commit)
fails on customer-identifying strings. Real customer schemas/data live ONLY in
`jazz-private`; `jazz_core` uses anonymized, name-blind fixtures (perf/lowering
gates are name-blind, so fidelity is preserved). Never commit real schema, dumps,
PII, or non-anonymized fixtures to this public repo.

**Perf loop.** Iterate perf on the in-repo native harness (anonymized fixture,
`cargo bench` under `[profile.perf]`) — not the workspace/NAPI/artifact-copy route,
which is milestone-only end-to-end validation. Every perf receipt emits its own
phase breakdown (attribution-by-default). Lanes should end reports with a
one-line **tooling-friction** note: what setup would have saved wall-clock.
