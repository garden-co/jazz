# Schema Files — Status Quo

This is the developer-facing layer for schema management. While the [Schema Manager](schema_manager.md) handles runtime concerns like multi-version reads, lens transforms, and schema catalogue sync, this layer is focused on what files developers edit locally and what `jazz-tools` commands they use to validate schemas and generate/push migrations.

## Project Layout

```text
app-root/
├── schema.ts
├── permissions.ts
└── migrations/
    └── 20260331-unnamed-aaaaaaaaaaaa-bbbbbbbbbbbb.ts
```

- `schema.ts` is the root structural schema.
- `permissions.ts` is optional and must be separate from `schema.ts`.
- Omitting `permissions.ts` means the project has no compiled policy bundle by
  default. Local runtimes that boot with structural schema only stay permissive
  locally until they learn a bundle, while runtimes with a loaded bundle enforce
  explicit row-policy grants only.
- `migrations/*.ts` contains reviewed migration modules.

## `jazz-tools validate`

`jazz-tools validate` compiles the schema into the runtime `wasmSchema` form to
make sure it is valid, checks permissions comply with the schema, and emits
explicit-policy diagnostics.

Current validation behavior:

- missing `permissions.ts` is treated as "zero explicit row grants" for
  diagnostics
- Jazz warns once per table x operation when explicit `read`, `insert`,
  `update`, or `delete` policies are missing
- missing explicit `delete` still warns even though enforcing runtimes may fall
  back to `update.using` at runtime
- malformed permissions, missing exports, and unknown tables still fail
  validation

## Migrations

Migrations are a separate workflow from validation.

The current mental model is:

- schemas are stored on the server and identified by hash
- `jazz-tools migrations create <fromHash> <toHash>` pulls two stored structural schemas and writes a typed migration stub into `migrations/`
- the developer reviews and edits that file
- `jazz-tools migrations push <fromHash> <toHash>` publishes the reviewed migration edge back to the server

Generated migrations use `defineMigration(...)` and carry:

- `fromHash`
- `toHash`
- `from` / `to` schema witnesses
- a `migrate` object that describes the structural steps

Example shape:

```typescript
import { schema as s } from "jazz-tools";

export default s.defineMigration({
  migrate: {
    // reviewed structural steps go here
  },
  fromHash: "aaaaaaaaaaaa",
  toHash: "bbbbbbbbbbbb",
  from: {
    /* schema witness */
  },
  to: {
    /* schema witness */
  },
});
```

## Key Files

| File                                              | Purpose                                                                |
| ------------------------------------------------- | ---------------------------------------------------------------------- |
| `packages/jazz-tools/src/cli.ts`                  | `validate`, `migrations`, `permissions`, and `schema export` commands  |
| `packages/jazz-tools/src/schema-loader.ts`        | Loads `schema.ts` and `permissions.ts`, then compiles to `wasmSchema`  |
| `packages/jazz-tools/src/schema-permissions.ts`   | Permission compilation, schema merge helpers, and validate diagnostics |
| `packages/jazz-tools/src/drivers/schema-wire.ts`  | Runtime schema envelope with the loaded-policy-bundle bit              |
| `packages/jazz-tools/src/migrations.ts`           | Typed migration DSL and forward-lens construction                      |
| `packages/jazz-tools/src/runtime/schema-fetch.ts` | Fetch/publish helpers for stored schemas and migrations                |
| `packages/jazz-tools/bin/jazz-tools.js`           | Top-level CLI wrapper and `build` rejection                            |
