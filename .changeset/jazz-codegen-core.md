---
"jazz-tools": patch
---

`jazz.app.json` declares computed fields (`"computed": { "<field>": { "type", "from" } }` per table) and transforms (top-level `"transforms": { "<name>": { "stored", "view" } }`, named by a column's `"transform"`), replacing `tsOnly`. A file written before alpha.66 with `tsOnly` or `slices` loads with a warning that they are ignored.

Loaders now refuse, naming the JSON path:

- a `"TODO: …"` placeholder left in `migrations`, a computed field's type or a transform's types;
- a relation named like a column of its table, or `id` (as the TS DSL refused);

A history version no chain of migrations connects to the current tables gets the implied identity step when its rows pass unchanged (index, merge-strategy or column-order changes, which earlier versions wrote no migration for); any other loads with a warning, and generating bindings refuses it.

The expression grammar reads `session.identity` (the exact login: `$createdBy.identity = session.identity`), and a `select` or `order by` list inside `recursive(…)` no longer runs into the next field, so the writer reads back the Gather policies it prints.

New `jazz-codegen` crate: typed bindings generated from `jazz.app.json` (`jazz.generated.ts`, embedding the compiled schema so runtimes never parse the readable file), and `migrations create` in Rust: it moves the version the committed bindings were generated from into `history` and writes the migration, leaving `"TODO: …"` where only the author can decide. jazz-napi exposes `generate`, `compileApp`, `schemaSourceHash`, `migrationsCreate`, `validateBindings` and `loadBackendApp`; `loadAppArtifact` also returns the permissions, the deployment body and warnings. The `jazz-tools` binary gains `generate [--check]` and `migrations create --name`, so apps without Node need nothing else.
