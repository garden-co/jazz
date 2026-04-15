# Codex Sessions Backend

Jazz-backed projection of live Codex rollout history into a canonical session,
turn, and `j` agent-run store.

This package is intended to mirror `~/.codex/sessions/**/*.jsonl` into a local
persistent Jazz store so higher-level tools can query active and completed `j`
sessions, then layer structured agent-run state on top of those same session
rows without introducing a second sidecar database.

## What it stores

- `codex_sessions`: one row per Codex session / rollout file
- `codex_sessions.project_root`, `latest_preview`, `latest_activity_at`, and
  related timestamps: stable summary fields for pickers and notifications
- `codex_turns`: one row per turn with user text, committed assistant text, and
  live partial assistant text
- `codex_sync_states`: projector watermarks keyed by rollout path
- `j_agent_definitions`: named agent or workflow definitions that `j` can invoke
- `j_agent_runs`: top-level agent executions keyed back to parent Codex sessions
- `j_agent_steps`: durable workflow step progress
- `j_agent_attempts`: child-session or retry attempts for each step
- `j_agent_waits`: explicit wait boundaries for event-driven resume
- `j_agent_session_bindings`: bindings from one logical agent run to related
  Codex sessions
- `j_agent_artifacts`: normalized outputs such as repo capsules or summaries

## What this implements now

The backend now supports the first storage slice from the Barnum-over-`j`
design:

- Codex session projection remains the raw-session ingest path
- `j_agent_*` rows live in the same Jazz app as the projected Codex sessions
- agent runs bind back to canonical Codex session rows by session id
- turns are upserted by `(session_id, turn_id)` instead of full delete/reinsert

The workflow engine and native `j` invocation layer are still higher-level
follow-up work. This package is the canonical database contract they should use.

## Commands

```sh
env GIT_CONFIG_GLOBAL=/dev/null CARGO_NET_GIT_FETCH_WITH_CLI=true pnpm --dir crates/jazz-napi build
pnpm --dir examples/codex-sessions-backend build:schema
pnpm --dir examples/codex-sessions-backend build
pnpm --dir examples/codex-sessions-backend test
pnpm --dir examples/codex-sessions-backend smoke
```

One-shot sync:

```sh
pnpm --dir examples/codex-sessions-backend sync \
  --codex-home ~/.codex \
  --data-path ./examples/codex-sessions-backend/codex-sessions.db
```

Targeted single-session sync:

```sh
node examples/codex-sessions-backend/dist/src/cli.js sync-session \
  --codex-home ~/.codex \
  --session-id <session-id> \
  --data-path ./examples/codex-sessions-backend/codex-sessions.db
```

`--data-path` names the Jazz store location. The runtime materializes a store
directory at that path, so do not pre-create a plain file there or startup will
fail.

Continuous watch:

```sh
pnpm --dir examples/codex-sessions-backend watch \
  --codex-home ~/.codex \
  --data-path ./examples/codex-sessions-backend/codex-sessions.db \
  --poll-interval-ms 1000
```

Completion event stream for Lin-style consumers:

```sh
pnpm --dir examples/codex-sessions-backend watch:completions -- \
  --codex-home ~/.codex \
  --data-path ./examples/codex-sessions-backend/codex-sessions.db \
  --poll-interval-ms 1000 \
  --bootstrap-window-ms 15000
```

This command keeps the rollout projection warm and emits one JSON line per
completed Codex turn with the project/session metadata Lin needs to surface a
completion badge without opening a custom URL.

Read-side query commands:

```sh
node examples/codex-sessions-backend/dist/src/cli.js list-sessions \
  --project-root ~/repos/openai/codex \
  --limit 10 \
  --data-path ./examples/codex-sessions-backend/codex-sessions.db

node examples/codex-sessions-backend/dist/src/cli.js list-active-runs \
  --project-root ~/repos/openai/codex \
  --limit 10 \
  --data-path ./examples/codex-sessions-backend/codex-sessions.db

node examples/codex-sessions-backend/dist/src/cli.js list-runs-for-session \
  --session-id <session-id> \
  --limit 10 \
  --data-path ./examples/codex-sessions-backend/codex-sessions.db

node examples/codex-sessions-backend/dist/src/cli.js get-run-summary \
  --run-id <run-id> \
  --data-path ./examples/codex-sessions-backend/codex-sessions.db
```

Write-side controller commands:

```sh
printf '%s\n' '{
  "definitionId": "repo-capsule",
  "name": "repo-capsule",
  "version": "v1",
  "sourceKind": "barnum_ts",
  "entrypoint": "barnum/workflows/repo-capsule.ts"
}' | node examples/codex-sessions-backend/dist/src/cli.js upsert-definition \
  --data-path ./examples/codex-sessions-backend/codex-sessions.db

printf '%s\n' '{
  "runId": "run-1",
  "definitionId": "repo-capsule",
  "projectRoot": "~/repos/openai/codex",
  "repoRoot": "~/repos/openai/codex",
  "cwd": "~/repos/openai/codex",
  "requestedRole": "scan"
}' | node examples/codex-sessions-backend/dist/src/cli.js record-run-started \
  --data-path ./examples/codex-sessions-backend/codex-sessions.db

printf '%s\n' '{
  "runId": "run-1",
  "stepId": "step-1",
  "sequence": 1,
  "stepKey": "spawn-worker",
  "stepKind": "spawnChildSession"
}' | node examples/codex-sessions-backend/dist/src/cli.js record-step-started \
  --data-path ./examples/codex-sessions-backend/codex-sessions.db
```

All write commands accept JSON through `--input-json`, `--input-file`, or stdin
and return normalized JSON rows. The lifecycle surface now includes:

- `upsert-definition`
- `record-run-started`
- `record-run-completed`
- `record-step-started`
- `record-step-completed`
- `record-attempt-started`
- `record-attempt-completed`
- `record-wait-started`
- `resolve-wait`
- `bind-session`
- `record-artifact`

The projector reparses only rollout files whose mtime has advanced since the
last sync state row. It keeps the Codex rollout JSONL as the source of truth
and writes a cleaner query surface into Jazz.
