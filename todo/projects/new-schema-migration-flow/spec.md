# New Schema Migration Flow

## Problem

The current migration authoring flow is server-led: a developer changes `schema.ts`, runs the app, notices old data is missing, digs through server logs to discover schema hashes, then runs `jazz-tools migrations create <fromHash> <toHash>`. That makes migration authoring reactive instead of part of normal schema work. It also couples the default workflow to server state, runtime warnings, and network access. In practice, developers cannot create a migration until they have seen a warning.

The default workflow should instead be local and proactive. A developer who changes `schema.ts` should be able to run one command, offline, and get the next migration stub by diffing the current schema against the latest committed local schema snapshot. When recovery from server state is needed, the primitive should be "materialize the missing structural schema locally" rather than a special migration command.

## Solution

This project changes the migration flow from "discover hashes from the server, then author a migration" to "diff the current schema against the latest local snapshot, then author a migration". It introduces committed JSON snapshot files, changes `jazz-tools migrations create` to not require positional from/to hashes, and extends `jazz-tools schema export` so a developer can ask for a specific schema hash and have it loaded from the server.

### File Layout

```text
app-root/
├── schema.ts
├── permissions.ts
└── migrations/
    ├── 20260406T153045Z-rename-todo-title-a1b2c3d4e5f6-bbccddeeff00.ts
    └── snapshots/
        ├── 20260401T101500Z-a1b2c3d4e5f6...64chars.json
        └── 20260406T153045Z-bbccddeeff00...64chars.json
```

- `schema.ts`
  - remains the only structural schema file a developer edits directly
- `migrations/*.ts`
  - remains the reviewed migration edge files that are pushed to servers
- `migrations/snapshots/*.json`
  - are machine-generated, committed schema snapshots
  - are ordered by a sortable UTC timestamp prefix
  - contain the exact JSON emitted by `jazz-tools schema export --format json`

Committed snapshots are part of the repository and are reviewed alongside migrations.

### Snapshot Format

Each snapshot file stores:

- `schemaHash` in the filename
- `createdAt` in the filename
- the canonical structural schema JSON as the file contents

The file contents are exactly the pretty-printed JSON currently emitted by `jazz-tools schema export --format json`. There is no wrapper object, no TypeScript module, and no DSL reconstruction step.

Example shape:

```json
{
  "todos": {
    "columns": [
      {
        "name": "title",
        "column_type": { "type": "Text" },
        "nullable": false
      }
    ]
  }
}
```

The CLI defines the "latest snapshot" as the file in `migrations/snapshots/` with the lexicographically greatest timestamped filename.

### Breadboard: Normal Flow

1. A developer edits `schema.ts`.
2. The developer runs `jazz-tools migrations create`.
3. The CLI compiles the current schema from `schema.ts` into canonical structural JSON.
4. The CLI loads the latest local committed snapshot from `migrations/snapshots/`.
5. The CLI diffs latest snapshot JSON -> current schema JSON.
6. The CLI writes:
   - a new migration stub in `migrations/`
   - a new committed snapshot for the current schema in `migrations/snapshots/`
7. The developer reviews the migration and snapshot, commits both files, and later runs `jazz-tools migrations push`.

### Breadboard: First Run in a Repo

1. A developer runs `jazz-tools migrations create`.
2. The CLI finds no existing committed snapshots.
3. The CLI writes a single initial snapshot for the current schema.
4. The CLI prints that no migration was created because there was no previous local schema baseline.

This creates the baseline for future diffs.

### Breadboard: No Structural Changes

1. A developer runs `jazz-tools migrations create`.
2. The CLI loads the current schema and latest committed snapshot.
3. The current schema hash matches the latest snapshot hash.
4. The CLI prints a no-op message and writes nothing.

This should be the common result for permission-only changes or repeated runs without structural edits.

### `jazz-tools migrations create`

`jazz-tools migrations create` becomes a local command with no required positional hashes.

Behavior:

1. Load and compile `schema.ts`.
2. Normalize it into the canonical structural JSON form used for diffing.
3. Resolve the base schema:
   - by default, from the latest committed snapshot in `migrations/snapshots/` to the current schema
   - if `--fromHash <schemaHash>` and/or `--toHash <schemaHash>` are provided, get the matching JSON snapshots or else fetch them from the server
4. If no committed snapshot exists and `--fromHash` is not provided:
   - write a first committed snapshot only
   - do not generate a migration
5. If `--fromHash` or `--toHash` are provided and the requested schema is not available locally nor in the server:
   - fail with a clear message
6. If the resolved base schema hash equals the current schema hash:
   - print a no-op message
   - do not generate a migration or snapshot
7. Otherwise:
   - diff `baseSchema` -> `currentSchema`
   - generate a migration stub for `baseSchemaHash` -> `currentSchemaHash`
   - generate a new committed snapshot file for `currentSchemaHash`

Generated files:

```text
migrations/{timestamp}-unnamed-{fromHash}-{toHash}.ts
migrations/snapshots/{timestamp}-{toHash}.json
```

The timestamp is shared by both files so the generated pair is easy to recognize in review.

The generated migration file remains self-contained and keeps:

- `fromHash`
- `toHash`
- `from`
- `to`
- `migrate`

### `jazz-tools schema export`

`schema export` becomes the structural-schema materialization command. It can either be run explicitly, or be implicitly invoked when using `migration create [--fromHash]/[--toHash]`

Supported modes:

- `jazz-tools schema export --schema-dir <path>`
  - compiles the current root `schema.ts`
  - prints canonical structural JSON to stdout
- `jazz-tools schema export --schema-hash <hash>`
  - resolves a structural schema by hash
  - prints the same canonical structural JSON to stdout

`--schema-dir` and `--schema-hash` are mutually exclusive.

Behavior for `--schema-hash`:

1. Resolve the requested hash against local committed snapshots.
2. If a matching local file exists:
   - print it
   - do not hit the network
3. Otherwise:
   - fetch the stored schema for that hash from the server
   - write it to `migrations/snapshots/{schema-hash}.json`
   - print it

This keeps `schema export` as a pure way to ask, "give me the canonical structural schema for this source", whether the source is the working tree or a historical published schema.

### Breadboard: Historical Recovery

1. A developer sees that the server contains rows under schema hash `a1b2...`.
2. The developer runs `jazz-tools schema export --schema-hash a1b2...`.
3. If that schema is not already available locally, the CLI fetches it from the server saves the JSON snapshot.
4. The developer decides how to connect the exported schema to an existing schema and runs `jazz-tools migrations create --fromHash a1b2...`.
5. The CLI diffs the fetched historical schema -> current schema.
6. The CLI writes a normal migration stub. The snapshot is not generated, as it's already present from 2.

### Runtime Warnings

Missing-schema detection remains valuable, but it changes role. It is no longer responsible for teaching developers the normal migration authoring flow. Instead, it acts as a recovery warning when stored server data is incompatible with the currently queried schema graph.

When the runtime or server detects rows that are not visible because no lens path exists, it should emit an actionable warning that points to `schema export --schema-hash`, including the missing hash or hashes.

Expected guidance:

```text
Detected rows with schema versions not reachable from the current schema.
To materialize a missing schema locally, run `jazz-tools schema export --schema-hash <schemaHash>`.
Then generate a migration with `jazz-tools migrations create --fromHash <sourceHash> --toHash <targetHash>`.
```

### Developer Experience

The new mental model is:

- edit `schema.ts`
- run `jazz-tools migrations create`
- review the generated migration and committed snapshot
- push the migration explicitly later

If recovery is needed:

- run `jazz-tools schema export --schema-hash <schemaHash>`
- run `jazz-tools migrations create --fromHash <schemaHash>`
- review the generated migration and committed snapshot

The server remains the source of truth for published compatibility, but the local repository becomes the source of truth for normal migration authoring history. The server is consulted only when a developer explicitly asks to materialize a historical schema hash that is not already available locally.
