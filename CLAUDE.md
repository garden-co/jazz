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

**Test failures are your problem.** All work that lands on `main` is tested in CI, so pre-existing failures are extremely unlikely. If a test fails unexpectedly, do not assume it was already broken — investigate. The failure is most likely caused by an incorrect implementation or tests that haven't been updated to a changed API contract, though stale binaries are cheap to rule out and worth checking early. Use judgement: weigh how likely each cause is against how much effort it takes to verify. Surface the failure to the user only after you've genuinely exhausted these avenues.

**Don't rewrite existing tests without permission.** Existing tests encode decisions about what correct behaviour looks like. If the task explicitly involves changing behaviour, updating the tests to match is the right thing to do. But if a test is failing simply because the implementation diverges from what the test expects, rewriting the test to match the new behaviour is risky — the test may well be correct and the implementation wrong. Treat that as a human-in-the-loop decision: surface it to the user rather than resolving it unilaterally.

## Skills

Repo-local skills live in `.agents/skills/`. Check them proactively.

## Quick Capture: Ideas & Issues

Capture only. No shaping, no implementation, no re-asking what's already in the prompt. After every write to `specs/todo/`, run `bash scripts/update-todo.sh`.

### Ideas → `specs/todo/ideas/{priority}/{idea-name}.md`

Use `1_mvp/`, `2_launch/`, or `3_later/`. Default to `1_mvp/`.

Template:

```markdown
# {Idea Title}

## What

## Notes
```

After saving, list ideas by bucket with their `What` line. Plain English only. Ideas are uncommitted.

### Issues → `specs/todo/issues/{issue-name}.md`

For bugs and focused problems, not feature ideas. Use kebab-case filenames. Prefix `test_` for test-related issues.

Template:

```markdown
# {Issue Title}

## What

## Priority

[critical / high / medium / low / unknown]

## Notes
```

After saving, list issues with their `What` line. Do not investigate or fix.
