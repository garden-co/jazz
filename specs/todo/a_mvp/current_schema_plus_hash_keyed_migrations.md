# Current Schema + Hash-Keyed Migrations — TODO (MVP)

Replace the current frozen-schema + generated-`app.ts` workflow with a codegen-free TypeScript schema model:

- apps keep a single editable `schema.ts` as the current schema
- compatibility edges live in self-contained migration files keyed by exact schema hashes
- runtime warns loudly when old data becomes unreachable
- clients still advertise or publish their current schema definitions to connected servers
- developers create and push migrations explicitly through the CLI

This keeps app authoring centered on the current schema while still preserving the Schema Manager's graph-based lens model under the hood.

## Goals

- Remove the need for `schema/app.ts` codegen in app code.
- Make `schema.ts` the only schema file developers edit during normal feature work.
- Keep migration authoring type-facilitated without requiring full historical schema snapshots in source.
- Make missing compatibility visible immediately in dev and server logs.
- Let servers learn about new current schemas automatically from connecting clients.
- Keep cross-schema visibility tightly gated by explicit migration publication through the CLI.

## Non-Goals

- Auto-pushing migrations from browser or backend clients.
- Requiring developers to maintain a linear version history in source control.
- Solving migration squashing, promotion policy, or deployment orchestration in this spec.
- Inferring semantic transforms from schema annotations. Migration intent lives in explicit migration files.

## File Convention

Each app keeps its current schema in the app root:

```text
schema.ts
migrations/
  20260318-unnamed-a1b2c3d4e5f6-112233445566.ts
  20260322-rename-todo-title-112233445566-77889900aabb.ts
```

- `schema.ts`
  - Lives in the root folder of the app.
  - Exports the current schema used for typechecking in the app.
  - Does not contain historical schema snapshots.
- `migrations/{dateCreated}-{name}-{fromHash}-{toHash}.ts`
  - `dateCreated` is the local creation date in sortable form, for example `20260318`.
  - New files are generated with `unnamed` and developers replace it with a meaningful name before committing.
  - `fromHash` and `toHash` are exact schema hashes, not version numbers.

## Authoring Model

Developers change `schema.ts` directly to whatever shape they want next.

New writes immediately use the new schema hash. Old data remains on its original schema branches until a migration path exists. The runtime does not silently invent compatibility once data is already stored under a different hash.

When a client using a new `schema.ts` connects, it should still publish its current schema definition to the connected server if that hash is unknown there. This keeps schema discovery automatic without also making compatibility automatic.

The intended development loop is:

1. Edit `schema.ts`.
2. Restart or reconnect the app as usual.
3. If existing data becomes unreachable, the Schema Manager emits a loud warning naming the old and new hashes.
4. Run `npx jazz-tools@{currentVersion} migrations create {oldHash} {newHash}`.
5. Fill in `migrate()`, rename the file, then run `npx jazz-tools@{currentVersion} migrations push {oldHash} {newHash}`.

## Runtime Behavior

### Detecting Missing Compatibility

When the Schema Manager sees rows that exist under a schema hash with no lens path to the current schema, it should not silently hide this situation.

Instead it should:

- detect the unreachable rows during query execution or branch discovery
- count affected rows per table when feasible
- log a structured warning in the browser client or backend client
- forward upstream warnings from servers to connected clients where appropriate

The warning should look roughly like:

```text
Detected {N} rows of {tableName} with differing schema versions. To ensure data visibility and forward/backward compatibility please create a new migration with `npx jazz-tools@{currentVersion} migrations create {oldHash} {newHash}`
```

This is intentionally actionable and points to one concrete next step.

### Semantics

- New data is always written using the current schema from `schema.ts`.
- Clients may cause servers to learn about a new schema hash by publishing the current schema definition on connect.
- Old data is readable only if the server knows a lens path from its stored hash to the querying hash.
- Missing paths are warnings first, but they also mean partial invisibility of old data until the migration is authored and pushed.
- Under the hood, schema compatibility remains a graph. A migration file defines one explicit edge from `fromHash` to `toHash`.

### Schema Publication vs Migration Publication

This design intentionally separates schema discovery from compatibility activation.

- Current schema definitions may still be published automatically by clients.
- Migration edges must never be published automatically by clients.
- A server that learns about a new schema hash without also learning a connecting migration should treat that schema as known but not yet broadly compatible.

This is the gating mechanism:

- publishing a schema allows a server tier to recognize and store data written under that schema
- publishing a migration edge is what makes rows visible across schema boundaries

That means a new app build can start writing data under a new schema hash immediately, while forward and backward visibility remains explicitly controlled by `migrations push`.

## Migration File Shape

Each migration file is self-contained and type-checks on its own. It should include:

- `fromHash`
- `toHash`
- minimal typed `from` schema witness
- minimal typed `to` schema witness
- `migrate()`

Example shape:

```ts
import { col, defineMigration } from "jazz-tools";

export default defineMigration({
  fromHash: "a1b2c3d4e5f6",
  toHash: "112233445566",
  from: {
    users: {
      name: col.string(),
    },
    todos: {
      text: col.string(),
      owner: col.ref("users"),
    },
  },
  to: {
    users: {
      name: col.string(),
    },
    todos: {
      title: col.string(),
      owner: col.ref("users"),
      priority: col.int(),
    },
  },
  migrate: (m) =>
    m.table("todos", (t) => t.rename("text", "title").add("priority", col.int(), { default: 0 })),
});
```

The `from` and `to` objects are intentionally minimal. They exist to make the migration type-safe, not to reintroduce full versioned schema snapshots into source control.

## Type-Safety Model

Migration type safety should come from the self-contained edge definition:

- `fromHash` and `toHash` identify the exact graph edge at runtime.
- `from` constrains source-side operations such as `rename`, `drop`, or source table selection.
- `to` constrains destination-side operations such as `add` and final target shape validation.
- `migrate()` is checked against both witnesses.

At minimum, the TypeScript API should make these mistakes hard or impossible:

- renaming a column that is absent from `from`
- adding a column that is absent from `to`
- dropping a column that still exists in `to`
- referencing a table name that is absent from the relevant witness
- producing a lens whose target shape is incompatible with `to`

The type model does not need to prove every semantic property, but it should eliminate obvious structural mistakes.

## CLI Workflow

### `migrations create`

Command:

```bash
npx jazz-tools@{currentVersion} migrations create {oldHash} {newHash}
```

Behavior:

1. Connect to the configured Jazz server for the app.
2. Load both schema versions identified by `{oldHash}` and `{newHash}`.
3. Diff them in memory.
4. Generate a migration stub at:

```text
migrations/{dateCreated}-unnamed-{fromHash}-{toHash}.ts
```

The generated stub should:

- prefill `fromHash` and `toHash`
- prefill minimal `from` and `to` witnesses
- prefill obvious structural operations when the diff is unambiguous
- leave TODO-shaped placeholders in `migrate()` when intent is ambiguous

After generation, the CLI should print guidance to:

- fill in `migrate()`
- rename the file by replacing `unnamed`
- run `npx jazz-tools@{currentVersion} migrations push {oldHash} {newHash}`

### `migrations push`

Command:

```bash
npx jazz-tools@{currentVersion} migrations push {oldHash} {newHash}
```

Behavior:

- Load the matching migration file from `migrations/`.
- Validate that its `fromHash` and `toHash` match the requested hashes.
- Typecheck or compile it if needed.
- Serialize and push the resulting lens edge to the server.

This is now the only supported way to publish migrations. Browser clients and backend clients should never push migrations automatically.

## Schema Publication Workflow

Schema publication remains automatic and separate from migration publication.

When a client connects with a `schema.ts` whose hash is unknown to the target server, it should:

1. Send its current schema hash as part of normal schema negotiation.
2. If the server does not know that hash, send the canonical current schema definition.
3. Let the server persist that schema object and propagate it to upstream tiers using normal catalogue mechanisms.

This auto-publication path should apply only to schema definitions, not migration edges.

Practical effect:

- dev, staging, and production tiers can learn about newly deployed app schemas without a separate CLI step
- data written under the new schema can be accepted immediately
- cross-version visibility still does not change until a migration edge is pushed explicitly

## Client and Server Responsibilities

### Clients

- Use `schema.ts` for local typechecking and current-schema reads/writes.
- Publish the current schema definition automatically if the connected server does not know its hash yet.
- Surface missing-compatibility warnings locally.
- Forward server-emitted warnings to developer consoles where possible.
- Never push migrations directly.

### Server

- Stores schema and lens graph data used by the Schema Manager.
- Learns about new schema hashes from connecting clients and propagates those schema objects upstream as needed.
- Detects and reports missing paths affecting stored rows.
- Serves the full schema graph to the CLI when generating migration stubs.
- Accepts migration publication only from the CLI or another explicitly administrative path.

## Why Exact Hashes

This design uses hash-keyed edges rather than human version numbers because:

- schemas are already content-addressed in the runtime
- branching and merging naturally create non-linear history
- the underlying lens model is a graph, not a sequence
- exact hashes avoid ambiguity when multiple parallel schema variants exist

The migration filename includes both hashes so a developer can understand the edge at a glance without needing a separate version registry.

## Open Questions

- Whether `migrations push` should upload only the lens edge or also ensure both endpoint schemas exist on the server.
- How strict stub generation should be for ambiguous renames and type changes.
- Whether warnings should be rate-limited or deduplicated per `(tableName, oldHash, newHash)`.
- Whether the CLI should support a later `migrations squash` or `migrations promote` command for consolidating many dev-time edges before staging or production rollout.

## Relationship to the Status Quo

This replaces the current developer-facing workflow described in [Schema Files](../../status-quo/schema_files.md):

- no frozen versioned schema files in app source
- no generated `schema/app.ts`
- client-driven publication of current schemas remains
- no client-driven publication of migrations
- no assumption of a linear `v1 -> v2 -> v3` authoring model

The runtime Schema Manager still operates on content-addressed schemas and graph-based lenses. What changes is the developer workflow: current schema in one file, explicit hash-keyed migration edges in separate files, automatic schema publication from clients, and CLI-only migration publication.
