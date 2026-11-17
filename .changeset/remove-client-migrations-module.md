---
"jazz-tools": patch
---

Rust: `jazz::tools::deployment::published::client_migrations_module`, which read a `migrations/index.ts` from Rust, is removed with that module (alpha.66 keeps an app's history and migrations in `jazz.app.json`). Load the file with `jazz::tools::deployment::artifact::load_client`, or lower the compiled schema generated bindings embed with `published::lowered_client_migrations`. `artifact::client_migrations` is removed too; use `load_client`.

`jazz-napi` adds `schemaHash(engineSchemaJson)`, the engine's hash of a schema (not `schemaSourceHash`, which hashes the `jazz.app.json` file). The app-file loaders also refuse:

- a migration that moves a kept column's reference to another table (one that follows a table the same migration renames is fine);
- an index naming a column the table doesn't have, or a computed field.
