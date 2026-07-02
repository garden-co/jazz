# Jazz

Distributed, local-first relational database. Rust core, TypeScript client layers, WASM + NAPI + React Native bindings.

## Specs

Architecture docs live in `specs/`. Status-quo specs describe what's built;

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

Run `dev/benchmarks/smoke.sh` for any change touching protocol, engine, storage,
or benchmark harnesses. Any change to a public `jazz` type additionally gates the
full workspace, including examples.

This rule exists because previous misses stayed hidden too long: `four_tier`
was born-red for roughly nine commits; `large_blob_values_follow_ordinary_row_permissions`
was born-red at `e03780d70`; `jazz-server`'s `cli_dry_run` target rotted after a
core API evolution; and adding `SyncMessage::SubscribeRejected` broke jazz-sim
bench compilation two steps before the bench gate caught it.

**Don't rewrite existing tests without permission.** Existing tests encode decisions about what correct behaviour looks like. If the task explicitly involves changing behaviour, updating the tests to match is the right thing to do. But if a test is failing simply because the implementation diverges from what the test expects, rewriting the test to match the new behaviour is risky — the test may well be correct and the implementation wrong. Treat that as a human-in-the-loop decision: surface it to the user rather than resolving it unilaterally.
