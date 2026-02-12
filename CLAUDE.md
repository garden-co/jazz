# Jazz

Distributed, local-first relational database. Rust core (`crates/groove`), TypeScript client layers, WASM + NAPI bindings. SQL is available (subset, custom dialect) but most consumers use higher-level DSLs — what matters is relational semantics.

## Specs

Architecture docs live in `specs/`. Status-quo specs describe what's built; todo specs describe what's next.

Todo specs are organized by timeline in `specs/todo/`:

- `a_week_YYYY_MM_DD/` — this week's tasks (new folder each week)
- `b_mvp/` — must-have for first adopters
- `c_launch/` — public launch readiness
- `d_later/` — post-launch

Each file is a single, descriptively named topic. Read the filenames to get an overview.

## Work style

Communicate tersely without losing precision or warmth.

**This is a prototype.** We can break backcompat at any point before launch. When a new idea or design arrives, rework existing code as if the new input had been an assumption from the beginning — don't just bolt on net-new code.

**TDD: red then green.** Write the test first, watch it fail, then make it pass. Broken tests are valuable — they document what doesn't work yet. When writing tests, mentally black-box the thing under test: consider only type signatures and spec'd contracts, not implementation details. The less you peek at internals, the less likely you are to write tests that pass for the wrong reasons.

**E2E over unit tests.** Prefer high-level integration tests (SchemaManager, RuntimeCore) over internal-helper tests. Exception: tiny unit tests for isolated pure functions.

**Builds:** `pnpm build` (everything), `pnpm test` (everything), via turbo.

## Completion bias

Your agentic harness and training heavily bias you toward completion. You may feel like time is running out. It isn't. We are doing complex work across multiple sessions and team members. Going slow is expected.

Priorities, in order:

1. **Correctness and design fidelity** — implement the design, not a shortcut that compiles
2. **Honesty** — surface what's broken, what you don't understand, what doesn't match your expectations
3. **Clarity** — leave clear TODOs, clear red tests, clear summaries of where things stand
4. Completion (eventually, as a natural consequence of the above)

Concretely:

- Intermediate breakage during complex work is fine. Don't add shims or workarounds to make each micro-step compile.
- Never weaken assertions, add `#[cfg(test)]` backdoors, or explain away failures to reach "done." If a test fails, that's information — investigate or leave it red.
- Ask before moving to the next phase. "It compiles" is not "it works."
- It's OK to stop mid-plan with a clear summary. Don't rush to a false finish.

## After hard problems

When something was harder than expected, pause and reflect 5-whys-style: where did the difficulty actually come from? Was it a wrong assumption, a missing spec, an architectural gap? Write it down (in memory or as a spec update) so we don't repeat it.
