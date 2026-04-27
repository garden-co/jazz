# Jazz

Distributed, local-first relational database. Rust core, TypeScript client layers, WASM + NAPI bindings. SQL is available (subset, custom dialect) but most consumers use higher-level DSLs — what matters is relational semantics.

## Specs

Architecture docs live in `specs/`. Status-quo specs describe what's built;

## Work style

Communicate tersely without losing precision or warmth.

**This is a prototype.** We can break backcompat at any point before launch. When a new idea or design arrives, rework existing code as if the new input had been an assumption from the beginning — don't just bolt on net-new code.

**TDD: red then green.** Write the test first, watch it fail, then make it pass. Broken tests are valuable — they document what doesn't work yet. When writing tests, mentally black-box the thing under test: consider only type signatures and spec'd contracts, not implementation details. The less you peek at internals, the less likely you are to write tests that pass for the wrong reasons.

**Tests should read like real usage.** Prefer realistic fixtures (domain-shaped objects/metadata) and human actor names (`alice`, `bob`, etc.) over generic placeholders (`obj1`, `client1`, `a`, `b`) unless the test is specifically about abstract graph mechanics.

**E2E over unit tests.** Prefer high-level integration tests over internal-helper tests. Exception: tiny unit tests for isolated pure functions.

**Builds:** `pnpm build` (everything), `pnpm test` (everything), via turbo.

## Skills

Repo-local skills live in `.agents/skills/`. Check them proactively.

## Ideas & Issues

Use the `issues` skill for ideas, bugs, and focused problems. Jazz Cloud-backed skill state is the source of truth.

Capture only unless the user asks for shaping or implementation. Track work with `open`, `in_progress`, and `done` status.
