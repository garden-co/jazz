# Agent Infra Backend

Monorepo-local first slice of the agent-infra control-plane plan.

This example does three things:

- defines a first Jazz schema for structured agent operational data
- wraps `jazz-tools/backend` in a narrow backend-owned store API
- exercises the store with a small persistent local smoke path and test

The intended follow-up is to consume this from `~/run` rather than keep raw
filesystem scanning as the only read surface.

This local slice also adds Jazz-backed task records so the current Designer
queue can move out of markdown-only state and into queryable rows while still
supporting import from `~/do`.

## Commands

```sh
env GIT_CONFIG_GLOBAL=/dev/null CARGO_NET_GIT_FETCH_WITH_CLI=true pnpm --dir crates/jazz-napi build
pnpm --dir examples/agent-infra-backend build:schema
pnpm --dir examples/agent-infra-backend build
pnpm --dir examples/agent-infra-backend test
pnpm --dir examples/agent-infra-backend smoke
```

## Rust CLI Surface

The shared Rust CLI now exposes this store through `jazz-tools db`.

Example schema discovery:

```sh
cargo run -p jazz-tools --features cli -- db list-tables \
  --app-id run-agent-infra \
  --data-dir /path/to/agent-data.db
```

Example query:

```sh
cargo run -p jazz-tools --features cli -- db query \
  --app-id run-agent-infra \
  --data-dir /path/to/agent-data.db \
  --json '{"table":"agent_runs","relation_ir":{"TableScan":{"table":"agent_runs"}}}'
```

The Rust CLI reads the persisted schema catalogue from storage. The current
`jazz-tools db` and `agent-infra` commands do not bootstrap from
`--schema-dir` yet; that flag is reserved for future compatibility.

## Agent-Infra Commands

For run-agent control-plane use, prefer the domain CLI over raw table edits:

```sh
cargo run -p jazz-tools --features cli -- agent-infra record-run-started \
  --app-id run-agent-infra \
  --data-dir /path/to/agent-data.db \
  --json '{"runId":"run-1","agentId":"plan","status":"running"}'

cargo run -p jazz-tools --features cli -- agent-infra get-run-summary run-1 \
  --app-id run-agent-infra \
  --data-dir /path/to/agent-data.db
```

This is the intended surface for `~/run`:

- Jazz stores structured metadata and relationships.
- Files remain the payload layer for traces, semantic journals, artifacts, and workspace snapshots.
- The runtime writes stable `runId` records plus file references into Jazz instead of forcing every raw byte into database rows.

## Task Commands

Import the current Designer queue from `~/do` into Jazz:

```sh
pnpm --dir examples/agent-infra-backend build
node examples/agent-infra-backend/dist/src/cli.js sync-do-designer \
  --data-path ~/.jazz2/agent-infra.db
```

List or inspect Jazz-backed tasks:

```sh
node examples/agent-infra-backend/dist/src/cli.js list-tasks \
  --context designer \
  --status active,next \
  --data-path ~/.jazz2/agent-infra.db

node examples/agent-infra-backend/dist/src/cli.js get-task d-001 \
  --data-path ~/.jazz2/agent-infra.db
```

Project Jazz-backed Designer state back into `~/do`:

```sh
node examples/agent-infra-backend/dist/src/cli.js project-do-designer \
  --data-path ~/.jazz2/agent-infra.db
```

Direct Jazz task mutations can also project immediately:

```sh
node examples/agent-infra-backend/dist/src/cli.js upsert-task \
  --project-do-designer \
  --data-path ~/.jazz2/agent-infra.db \
  --input-file /tmp/task.json
```

## Prep-Workflow Run Commands

The Barnum-backed Designer prep workflow persists each slice here as an
`AgentRun` with staged `RunItem`s plus file artifacts.

Record or inspect those workflow slices through the domain CLI:

```sh
node examples/agent-infra-backend/dist/src/cli.js record-run-started \
  --data-path ~/.jazz2/agent-infra.db \
  --input-file /tmp/run-start.json

node examples/agent-infra-backend/dist/src/cli.js record-item-started \
  --data-path ~/.jazz2/agent-infra.db \
  --input-file /tmp/item-start.json

node examples/agent-infra-backend/dist/src/cli.js record-artifact \
  --data-path ~/.jazz2/agent-infra.db \
  --input-file /tmp/artifact.json

node examples/agent-infra-backend/dist/src/cli.js record-workspace-snapshot \
  --data-path ~/.jazz2/agent-infra.db \
  --input-file /tmp/workspace-snapshot.json

node examples/agent-infra-backend/dist/src/cli.js record-run-completed \
  --data-path ~/.jazz2/agent-infra.db \
  --input-file /tmp/run-complete.json

node examples/agent-infra-backend/dist/src/cli.js get-run-summary \
  --run-id designer-prep-d-004-phase-0-20260408T204154Z \
  --data-path ~/.jazz2/agent-infra.db
```

These commands are JSON-in / JSON-out on purpose so Barnum and the Go
front-door agents can call them deterministically without scraping human text.

## Remote Jazz Storage

The same domain CLI can target a remote Jazz sync server. Publish the generated
agent-infra schema first, then pass the remote connection flags to normal write
commands:

```sh
node examples/agent-infra-backend/dist/src/cli.js publish-schema \
  --app-id "$JAZZ2_AGENT_INFRA_APP_ID" \
  --server-url "$JAZZ2_AGENT_INFRA_SERVER_URL" \
  --admin-secret "$JAZZ2_AGENT_INFRA_ADMIN_SECRET" \
  --data-path ~/.jazz2/agent-infra.db
```

All commands accept:

- `--app-id` or `JAZZ2_AGENT_INFRA_APP_ID`
- `--server-url` or `JAZZ2_AGENT_INFRA_SERVER_URL`
- `--backend-secret`, `--backend-secret-env`, or `JAZZ2_AGENT_INFRA_BACKEND_SECRET`
- `--admin-secret`, `--admin-secret-env`, or `JAZZ2_AGENT_INFRA_ADMIN_SECRET`
- `--jazz-env`, `--user-branch`, `--tier`, and `--data-path`

When `--server-url` is set, backend-owned store operations use
backend-authenticated sync through `backendSecret`.

## Designer CAD Commands

The Designer CAD surface stores collaborative `.build123d.py` work as a
workspace/document/session plus operation, source-edit, preview, widget, steer,
and event rows. This is the JSON surface for Prom Designer and Codex/OpenClaw
harnesses:

```sh
node examples/agent-infra-backend/dist/src/cli.js record-designer-cad-workspace \
  --data-path ~/.jazz2/agent-infra.db \
  --input-json '{"workspaceId":"workspace-build123d","workspaceKey":"prom-designer","repoRoot":"~/code/prom","workspaceRoot":"~/code/prom/ide/designer"}'

node examples/agent-infra-backend/dist/src/cli.js record-designer-cad-document \
  --data-path ~/.jazz2/agent-infra.db \
  --input-json '{"workspaceId":"workspace-build123d","documentId":"doc-bracket","filePath":"workspace/bracket.build123d.py"}'

node examples/agent-infra-backend/dist/src/cli.js record-designer-cad-session \
  --data-path ~/.jazz2/agent-infra.db \
  --input-json '{"cadSessionId":"cad-session-1","workspaceId":"workspace-build123d","documentId":"doc-bracket","codexSessionId":"codex:019deb0a-d19b-7d92-81dd-76612d076d4c","openedBy":"alice"}'

node examples/agent-infra-backend/dist/src/cli.js record-designer-cad-operation \
  --data-path ~/.jazz2/agent-infra.db \
  --input-json '{"operationId":"op-1","cadSessionId":"cad-session-1","actorKind":"agent","actorId":"codex:019defcc-a8da-76d0-942e-b0dbaff55f86","operationKind":"source.patch","status":"validated","operationJson":{"filePath":"workspace/bracket.build123d.py"}}'

node examples/agent-infra-backend/dist/src/cli.js get-designer-cad-session-summary \
  --data-path ~/.jazz2/agent-infra.db \
  --cad-session-id cad-session-1
```

Related commands:

- `record-designer-cad-event`
- `upsert-designer-cad-scene-node`
- `upsert-designer-cad-selection`
- `record-designer-cad-tool-session`
- `record-designer-cad-source-edit`
- `record-designer-cad-preview-handle`
- `record-designer-cad-preview-update`
- `record-designer-cad-widget`
- `record-designer-cad-steer`
- `list-designer-cad-events`
- `list-designer-cad-operations`
