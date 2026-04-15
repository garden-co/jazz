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
- `migrations/*.ts` contains reviewed migration modules.

## `jazz-tools validate`

`jazz-tools validate` compiles the schema into the runtime `wasmSchema` form to make sure it is valid, and checks permissions comply with the schema.

## Migrations

Migrations are a separate workflow from validation.

The current mental model is:

- schemas are stored on the server and identified by hash
- `jazz-tools migrations create --fromHash <fromHash> --toHash <toHash>` pulls two stored structural schemas and writes a typed migration stub into `migrations/`
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

| File                                              | Purpose                                                               |
| ------------------------------------------------- | --------------------------------------------------------------------- |
| `packages/jazz-tools/src/cli.ts`                  | `validate`, `migrations`, `permissions`, and `schema export` commands |
| `packages/jazz-tools/src/schema-loader.ts`        | Loads `schema.ts` and `permissions.ts`, then compiles to `wasmSchema` |
| `packages/jazz-tools/src/migrations.ts`           | Typed migration DSL and forward-lens construction                     |
| `packages/jazz-tools/src/runtime/schema-fetch.ts` | Fetch/publish helpers for stored schemas and migrations               |
| `packages/jazz-tools/bin/jazz-tools.js`           | Top-level CLI wrapper and `build` rejection                           |
