# Jazz Tools Dev Catalogue API Design

## Context

`packages/jazz-tools/src/cli.ts` currently owns the complete deployment workflow:

- load the local `schema.ts` and optional `permissions.ts`
- detect whether the structural schema is already stored
- publish a new structural schema when needed
- check whether the previous permissions head is connected to the new schema
- publish the required migration edge when the server reports a missing path
- publish the current permissions bundle

`packages/jazz-tools/src/dev/dev-server.ts` exposes `pushSchemaCatalogue`, used by the dev plugins, schema watcher, testing helpers, examples, and integration tests. That helper loads the same project files, but publishes schema and permissions directly without the richer deployment checks that exist in the CLI.

The goal is to make the common catalogue operations usable programmatically from `jazz-tools/dev`, while keeping local development auto-push fast and preserving existing public API compatibility.

## Public API

Export the project-level catalogue operations from `jazz-tools/dev`:

```ts
export async function pushSchema(options: PushSchemaOptions): Promise<PushSchemaResult>;
export async function pushPermissions(
  options: PushPermissionsOptions,
): Promise<PushPermissionsResult>;
export async function pushMigration(options: PushMigrationOptions): Promise<PushMigrationResult>;
export async function deploy(options: DeployOptions): Promise<DeployResult>;
```

Keep `pushSchemaCatalogue` as a compatibility alias exported from `jazz-tools/dev` and `jazz-tools/testing`. Existing callers should continue to work without changing imports or behavior.

These functions are project-level APIs: they operate on `schemaDir` and `migrationsDir`, load `schema.ts` / `permissions.ts`, and apply Jazz's deployment rules. They are distinct from the lower-level HTTP helpers in `runtime/schema-fetch.ts`, which publish already-compiled schema, permissions, and migration payloads.

## Operation Semantics

`pushSchema` loads the local structural schema and publishes it to the target server. It should support a `skipIfStored` option used by `deploy`, so production-style deploys can avoid creating duplicate stored schema objects. `skipIfStored` defaults to `false` so `pushSchemaCatalogue` keeps the current development helper behavior.

`pushPermissions` loads `permissions.ts`, resolves the target schema hash from either an explicit `schemaHash` or the stored structural schema that matches local `schema.ts`, reads the current permissions head, and publishes the new bundle with `expectedParentBundleObjectId`.

`pushMigration` keeps the current CLI migration behavior: resolve short or full hashes against the server, find a matching local migration file, validate its exported hashes and schema witnesses, serialize the forward lens operations, and publish the edge. If no file exists and the structural transition does not require row transforms, publish an empty migration edge.

`deploy` orchestrates the production-safe flow:

1. Load the local project and emit missing explicit-policy diagnostics as warnings.
2. Find the stored structural schema matching local `schema.ts`; publish it only when missing.
3. If there is no `permissions.ts`, stop after schema publication.
4. Read the current permissions head.
5. If the current permissions head targets another schema hash, ask the server whether a migration path already connects it to the local schema hash.
6. If not connected, call `pushMigration`.
7. If the migration is missing, fail by default with the same actionable message as the CLI; allow a warning-only path with `noVerify` for the existing CLI flag.
8. Publish current permissions against the local structural schema hash.

The development plugin and schema watcher should keep using `pushSchemaCatalogue`, not `deploy`, so file-save auto-push remains lightweight and does not attempt migration verification on every edit.

## Results And Events

The programmatic API should not require callers to intercept `console` output. Each function should return structured results and accept an optional `onEvent` callback:

```ts
type CatalogueEvent =
  | { type: "schema-loaded"; schemaFile: string }
  | { type: "schema-published"; hash: string }
  | { type: "schema-skipped"; hash: string; reason: "already-stored" }
  | { type: "permissions-loaded"; permissionsFile: string }
  | { type: "permissions-published"; schemaHash: string; version?: number }
  | { type: "migration-published"; fromHash: string; toHash: string; filePath?: string }
  | { type: "warning"; message: string };
```

Implementation may add more event variants, but it must preserve this boundary:

- the dev/catalogue layer reports facts and warnings
- the CLI formats those facts as human-readable output
- tests can assert result objects instead of captured stdout where practical

## Module Layout

Add the reusable module:

```text
packages/jazz-tools/src/dev/catalogue.ts
```

Move or share the catalogue-specific logic currently embedded in `cli.ts`:

- schema equality and stored-schema lookup
- permissions-head formatting inputs
- migration-file resolution and migration publishing
- deploy orchestration

Keep CLI-only concerns in `cli.ts`:

- argument parsing
- environment-file loading
- usage text
- stdout/stderr formatting
- process exit handling

Some helpers in `cli.ts` are also used by migration stub generation and schema export. Those should only move if they are needed by the new programmatic API; unrelated CLI behavior should stay scoped to the CLI file.

## Backward Compatibility

`pushSchemaCatalogue` remains exported from:

- `jazz-tools/dev`
- `jazz-tools/testing`

Its default behavior should remain compatible with current tests and examples: load the project, publish the structural schema, publish permissions when present, and return `{ hash }`.

`deploy` remains available to the CLI command with the same command-line behavior and messages. Programmatic callers should import it from `jazz-tools/dev` after this change.

No JSON-like schema, permissions, or query definitions should be introduced in tests. New tests should continue using the public Jazz schema and permissions APIs.

## Testing

Prefer black-box integration-style tests around the public APIs:

- `jazz-tools/dev` exports the new functions and retains `pushSchemaCatalogue`.
- `pushSchemaCatalogue` preserves current behavior for existing test fixtures.
- `deploy` returns structured results for: schema-only publish, schema + permissions publish, already-stored schema, migration-required publish, missing migration failure, and `noVerify` warning.
- CLI deploy still prints the current user-facing messages by mapping API events/results to console output.

Existing tests should not be rewritten wholesale. Update targeted assertions only where the implementation boundary changes from direct CLI internals to the shared dev API.

## Impact

This design reduces duplicated schema/permissions publication logic between CLI and dev helpers, makes `deploy` usable by scripts and higher-level tooling, and keeps the local dev server startup path separate from production-style deployment verification.

The main implementation risk is accidentally changing auto-push behavior in dev mode. Keeping `pushSchemaCatalogue` as a compatibility wrapper with the current defaults limits that risk.
