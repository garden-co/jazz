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

## Designer State And Object Storage

Designer stores authoritative state in Jazz rows and stores every large,
immutable, or replayable payload in object storage. Jazz rows keep typed IDs,
causal sequence fields, summary text, status, timestamps, and object references.
Object storage keeps Codex transcripts, turn payloads, prompts, responses,
usage telemetry payloads, indexer replay payloads, CAD source snapshots, preview
artifacts, and any future Designer-produced binary or JSON artifact.

The first shared surface for Prom Designer and remote Codex is object-backed
conversation capture:

```sh
node examples/agent-infra-backend/dist/src/cli.js record-designer-object-ref \
  --data-path ~/.jazz2/agent-infra.db \
  --input-json '{"objectRefId":"obj-transcript-019dec01","provider":"oci","uri":"oci://designer-codex/conversations/019dec01/transcript.jsonl","objectKind":"codex.transcript","contentType":"application/jsonl"}'

node examples/agent-infra-backend/dist/src/cli.js record-designer-codex-conversation \
  --data-path ~/.jazz2/agent-infra.db \
  --input-json '{"conversationId":"designer-codex-019dec01","provider":"codex","providerSessionId":"019dec01-6eaa-7650-986f-f41ab49a59fd","workspaceKey":"rubiks-cube","workspaceRoot":"~/code/prom/ide/designer","transcriptObjectRefId":"obj-transcript-019dec01"}'

node examples/agent-infra-backend/dist/src/cli.js record-designer-object-ref \
  --data-path ~/.jazz2/agent-infra.db \
  --input-json '{"objectRefId":"obj-turn-019dec01-0001","provider":"oci","uri":"oci://designer-codex/conversations/019dec01/turns/0001.json","objectKind":"codex.turn","contentType":"application/json"}'

node examples/agent-infra-backend/dist/src/cli.js record-designer-codex-turn \
  --data-path ~/.jazz2/agent-infra.db \
  --input-json '{"conversationId":"designer-codex-019dec01","sequence":1,"turnKind":"assistant","role":"assistant","actorKind":"agent","payloadObjectRefId":"obj-turn-019dec01-0001","summaryText":"Object-backed response payload."}'

node examples/agent-infra-backend/dist/src/cli.js record-designer-object-ref \
  --data-path ~/.jazz2/agent-infra.db \
  --input-json '{"objectRefId":"obj-telemetry-019dec01-0001","provider":"oci","uri":"oci://designer-telemetry/events/019dec01/0001.json","objectKind":"designer.telemetry.event","contentType":"application/json"}'

node examples/agent-infra-backend/dist/src/cli.js record-designer-telemetry-event \
  --data-path ~/.jazz2/agent-infra.db \
  --input-json '{"conversationId":"designer-codex-019dec01","eventType":"designer.agent_prompt_completed","pane":"chat","sequence":1,"payloadObjectRefId":"obj-telemetry-019dec01-0001"}'

node examples/agent-infra-backend/dist/src/cli.js get-designer-codex-conversation-summary \
  --data-path ~/.jazz2/agent-infra.db \
  --conversation-id designer-codex-019dec01
```

Live Prom commits are also Jazz rows. The live-commit courier should record one
`designer_live_commits` row for every proven live commit, while patch/manifest
payloads stay in object storage. Designer can subscribe to those rows by
`repoRoot`, `branch`, `sourceSessionId`, or `agentId` instead of watching the
filesystem.

```sh
node examples/agent-infra-backend/dist/src/cli.js record-designer-agent \
  --data-path ~/.jazz2/agent-infra.db \
  --input-json '{"agentId":"agent.remote-codex.designer","agentKind":"codex","provider":"openai-codex","displayName":"Remote Codex Designer","model":"gpt-5.5","defaultContextJson":{"repoRoot":"~/code/prom","workspaceRoot":"~/code/prom/ide/designer"}}'

node examples/agent-infra-backend/dist/src/cli.js record-designer-agent-tool \
  --data-path ~/.jazz2/agent-infra.db \
  --input-json '{"toolId":"agent.remote-codex.designer:tool:apply_patch","agentId":"agent.remote-codex.designer","toolName":"apply_patch","toolKind":"workspace.edit","scopeJson":{"allowedPathPrefixes":["ide/designer"]}}'

node examples/agent-infra-backend/dist/src/cli.js record-designer-object-ref \
  --data-path ~/.jazz2/agent-infra.db \
  --input-json '{"objectRefId":"obj-live-commit-01f4d1e-patch","provider":"oci","uri":"oci://designer-commits/prom/live/01f4d1e.patch","objectKind":"vcs.commit.patch","contentType":"text/x-diff"}'

node examples/agent-infra-backend/dist/src/cli.js record-designer-live-commit \
  --data-path ~/.jazz2/agent-infra.db \
  --input-json '{"commitId":"01f4d1ea1cea8f331c1691a3312c6df1043db08b","repoRoot":"~/code/prom","workspaceRoot":"~/code/prom/ide/designer","branch":"live","bookmark":"nikiv-live","subject":"fix(designer): harden remote codex chat replay","traceRef":"codex:1_eyJzIjoiMDE5ZGViMGEtZDE5Yi03ZDkyLTgxZGQtNzY2MTJkMDc2ZDRjIiwidCI6MX0","sourceSessionId":"codex:019deb0a-d19b-7d92-81dd-76612d076d4c","sourceTurnOrdinal":1,"agentId":"agent.remote-codex.designer","patchObjectRefId":"obj-live-commit-01f4d1e-patch","status":"reflected","committedAt":"2026-05-03T22:53:03Z","reflectedAt":"2026-05-03T22:54:34Z"}'

node examples/agent-infra-backend/dist/src/cli.js list-designer-live-commits \
  --data-path ~/.jazz2/agent-infra.db \
  --repo-root ~/code/prom \
  --branch live
```

The Designer telemetry branch maps into this shape directly:

- `UsageTelemetryEvent` becomes `designer_telemetry_events`.
- Live trace events keep their compact row shape, with full payloads stored as
  `designer_object_refs`.
- Indexer upload receipts map to `designer_object_refs.metadata_json` and the
  object URI fields.
- Codex/app-server thread tails map to `designer_codex_conversations` and
  `designer_codex_turns`, with transcript and turn bodies always stored behind
  object refs.

Related commands:

- `record-designer-object-ref`
- `record-designer-agent`
- `record-designer-agent-tool`
- `record-designer-agent-context`
- `record-designer-codex-conversation`
- `record-designer-codex-turn`
- `record-designer-telemetry-event`
- `record-designer-live-commit`
- `list-designer-codex-turns`
- `list-designer-telemetry-events`
- `list-designer-agent-tools`
- `list-designer-agent-contexts`
- `list-designer-live-commits`
- `get-designer-codex-conversation-summary`
- `get-designer-live-commit-summary`

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
