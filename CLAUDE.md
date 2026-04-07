# Jazz

Distributed, local-first relational database. Rust core, TypeScript client layers, WASM + NAPI bindings. SQL is available (subset, custom dialect) but most consumers use higher-level DSLs — what matters is relational semantics.

## Specs

Architecture docs live in `specs/`. Status-quo specs describe what's built;

## Work style

Communicate tersely without losing precision or warmth.

**This is a prototype.** We can break backcompat at any point before launch. When a new idea or design arrives, rework existing code as if the new input had been an assumption from the beginning — don't just bolt on net-new code.

**TDD: red then green.** Write the test first, watch it fail, then make it pass. Broken tests are valuable — they document what doesn't work yet. When writing tests, mentally black-box the thing under test: consider only type signatures and spec'd contracts, not implementation details. The less you peek at internals, the less likely you are to write tests that pass for the wrong reasons.

**E2E over unit tests.** Prefer high-level integration tests over internal-helper tests. Exception: tiny unit tests for isolated pure functions.

**Builds:** `pnpm build` (everything), `pnpm test` (everything), via turbo.

## Skills

Repo-local skills live in `.agents/skills/`. Check them proactively.

## Quick Capture: Ideas & Issues

Capture only. No shaping, no implementation, no re-asking what's already in the prompt. After every write to `todo/`, run `bash scripts/update-todo.sh`.

### Ideas → `todo/ideas/{priority}/{idea-name}.md`

Use `1_mvp/`, `2_launch/`, or `3_later/`. Default to `1_mvp/`.

Template:

```markdown
# {Idea Title}

## What

## Notes
```

After saving, list ideas by bucket with their `What` line. Plain English only. Ideas are uncommitted.

### Issues → `todo/issues/{issue-name}.md`

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
