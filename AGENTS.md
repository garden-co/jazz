# Jazz

Distributed, local-first relational database. Rust core, TypeScript client layers, WASM + NAPI + React Native bindings.

## Specs

Architecture docs live in `specs/`. Status-quo specs describe what's built;

## Work style

**Testing:** prefer black-boxed integration tests over unit tests or white-box tests.
Do not use JSON-like schema/permissions/query definitions. Always use the public API to build them in the tests.
Before writing any test in Rust crates, always read `crates/jazz-tools/TESTING_GUIDELINES.md` in full and follow it.

**Builds:** `pnpm build:core` (all the packages), `pnpm test` (everything), via turbo.

**Don't rewrite existing tests without permission.** Existing tests encode decisions about what correct behaviour looks like. If the task explicitly involves changing behaviour, updating the tests to match is the right thing to do. But if a test is failing simply because the implementation diverges from what the test expects, rewriting the test to match the new behaviour is risky — the test may well be correct and the implementation wrong. Treat that as a human-in-the-loop decision: surface it to the user rather than resolving it unilaterally.
