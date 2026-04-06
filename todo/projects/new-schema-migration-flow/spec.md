# New Schema Migration Flow

## Problem

The current migration authoring flow is server-led: a developer changes `schema.ts`, runs the app, notices old data is missing, digs through server logs to discover schema hashes, then runs `jazz-tools migrations create <fromHash> <toHash>`. That makes migration authoring reactive instead of part of normal schema work. It also couples the default workflow to server state, runtime warnings, and network access (when working with a remote server). In practice, developers cannot create a migration until they have either seen a warning.

The default workflow should instead be local and proactive. A developer who changes `schema.ts` should be able to run one command, offline, and get the next migration stub by diffing the current schema against the latest committed local schema snapshot. Server-aware recovery should still exist, but it should be an explicit fallback path rather than the only way to learn what migration to write.

## Solution

This spec changes the migration flow from "discover hashes from the server, then author a migration" to "diff the current schema against the latest local snapshot, then author a migration". It introduces committed schema snapshot files, changes `jazz-tools migrations create` to not require from/to hashes and be local-only, and keeps a separate `jazz-tools migrations create-missing` command for server-oriented recovery when the missing compatibility is discovered from live data.

### File Layout

```text
app-root/
├── schema.ts
├── permissions.ts
└── migrations/
    ├── 20260406T153045Z-rename-todo-title-a1b2c3d4e5f6-bbccddeeff00.ts
    └── snapshots/
        ├── 20260401T101500Z-a1b2c3d4e5f6.ts
        └── 20260406T153045Z-bbccddeeff00.ts
```

- `schema.ts`
  - remains the only structural schema file a developer edits directly
- `migrations/*.ts`
  - remains the reviewed migration edge files that are pushed to servers
- `migrations/snapshots/*.ts`
  - are machine-generated, committed schema snapshots
  - are ordered by a sortable UTC timestamp prefix
  - contain a schema snapshot expressed with the same DSL family as `schema.ts`, plus the full schema hash

Snapshot files are part of the repository and are reviewed alongside migrations.

### Snapshot Format

Each snapshot file stores:

- `schemaHash` (in filename)
- `createdAt` (in filename)
- `schema`

`schema` is stored as a generated TypeScript module using the same DSL family as `schema.ts`. The snapshot is still machine-oriented rather than hand-edited. The CLI compiles that snapshot back into the canonical structural schema form before diffing, so diffing remains normalization-based even though the committed artifact is TypeScript.

Example shape:

```ts
import { schema as s } from "jazz-tools";

export const snapshot = {
  todos: s.table({
    title: s.string(),
  }),
};
```

The CLI defines the "latest snapshot" as the snapshot file in `migrations/snapshots/` with the lexicographically greatest timestamped filename.

### Breadboard: Normal Flow

1. A developer edits `schema.ts`.
2. The developer runs `jazz-tools migrations create`.
3. The CLI loads the current compiled schema from `schema.ts`.
4. The CLI loads the latest local snapshot from `migrations/snapshots/`.
5. The CLI diffs latest snapshot -> current schema.
6. The CLI writes:
   - a new migration stub in `migrations/`
   - a new snapshot for the current schema in `migrations/snapshots/`
7. The developer reviews the migration, renames the file, commits both files, and later runs `jazz-tools migrations push`.

### Breadboard: First Run in a Repo

1. A developer runs `jazz-tools migrations create`.
2. The CLI finds no existing local snapshots.
3. The CLI writes a single initial snapshot for the current schema.
4. The CLI prints that no migration was created because there was no previous local schema baseline.

This creates the baseline for future diffs.

### Breadboard: No Structural Changes

1. A developer runs `jazz-tools migrations create`.
2. The CLI loads the current schema and latest snapshot.
3. The current schema hash matches the latest snapshot hash.
4. The CLI prints a no-op message and writes nothing.

This should be the common result for permission-only changes or repeated runs without structural edits.

### `jazz-tools migrations create`

`jazz-tools migrations create` becomes a local, hashless command.

Behavior:

1. Load and compile `schema.ts`.
2. Normalize it into the canonical structural form used for diffing.
3. Load the latest local snapshot from `migrations/snapshots/`.
4. If no snapshot exists:
   - write a first snapshot only
   - do not generate a migration
5. Otherwise, compile the snapshot module into the same canonical structural form.
6. If the latest snapshot hash equals the current schema hash:
   - print a no-op message
   - do not generate a migration or snapshot
7. Otherwise:
   - diff `latestSnapshot.schema` -> `currentSchema`
   - generate a migration stub for `latestSnapshot.schemaHash` -> `currentSchemaHash`
   - generate a new snapshot file for `currentSchemaHash`

Generated files:

```text
migrations/{timestamp}-unnamed-{fromHash}-{toHash}.ts
migrations/snapshots/{timestamp}-{toHash}.ts
```

The timestamp is shared by both files so the generated pair is easy to recognize in review.

The generated migration file remains self-contained and keeps:

- `fromHash`
- `toHash`
- `from`
- `to`
- `migrate`

As a follow-up, we could make migrations reference snapshots in `to`/`from` to reduce duplication and make migrations less verbose.

### `jazz-tools migrations create-missing`

`create-missing` becomes the explicit server-diff workflow.

It exists for cases where:

- old data is already present on a server under a schema absent from local snapshots
- a developer is recovering from an incomplete migration history
- a team wants to inspect the server’s missing compatibility state directly

It generates one direct migration stub from each server-known schema not reachable from the server’s current schema.

### Runtime Warnings

Missing-schema detection remains valuable, but it changes role. It is no longer responsible for teaching developers the normal migration authoring flow. Instead, it acts as a recovery warning when stored server data is incompatible with the currently queried schema graph.

When the runtime or server detects rows that are not visible because no lens path exists, it should emit an actionable warning that points to `create-missing`, not to `create <fromHash> <toHash>`.

Expected guidance:

```text
Detected rows with schema versions not reachable from the current schema. To generate recovery migration stubs, run `jazz-tools migrations create-missing`
```

### Developer Experience

The new mental model is:

- edit `schema.ts`
- run `jazz-tools migrations create`
- review the generated migration and snapshot
- push the migration explicitly later

The server remains the source of truth for published compatibility, but the local repository becomes the source of truth for normal migration authoring history.

## Gotchas

- Timestamp ordering is convenient but imperfect. If developers create snapshots on branches with skewed clocks or reorder history during rebases, "latest snapshot" by filename may not match the intended lineage. The CLI must be deterministic and report which snapshot it selected, and also allow explicitly setting a "from" schema
