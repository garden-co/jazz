# Todo Project Cutover Decisions

The tracked `todo/projects/` files were reviewed during the skill-issues cutover.

## Decisions

- `ordered-index-topk-query-path` is durable project planning/spec intent, not a single importable issue or idea. It moved to `docs/superpowers/specs/ordered-index-topk-query-path/` as `problem.md`, `pitch.md`, and `scopes.md`.
- `relational-row-history-engine` is durable architecture and roadmap intent. It moved to `docs/superpowers/specs/relational-row-history-engine/` as `spec.md`, `pitch.md`, and `scopes.md`.

The remaining tracked `todo/ideas` and `todo/issues` markdown files are generated/export-compatible item source. Their import behavior is covered by `examples/skill-issues/tests/current-import-fixture.test.ts` before the tracked markdown deletion and by an equivalent fixture after the deletion.

## Cutover Checks

- `pnpm --filter skill-issues cli import todo` was attempted and blocked by `SKILL_ISSUES_APP_ID is required.`
- `pnpm --filter skill-issues cli list` was attempted and blocked by `SKILL_ISSUES_APP_ID is required.`
- `pnpm --filter skill-issues cli export todo` was attempted and blocked by `SKILL_ISSUES_APP_ID is required.`
- `git check-ignore -v` verified exported `todo/` output plus root and package `.skill-issues/` config paths are ignored.
