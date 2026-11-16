# `jazz.app.json` and `jazz.permissions.json` fixtures

Canonical files written by `jazz::tools::deployment::artifact::write`, one directory per app, as they sit beside its `schema.ts`.
Every language's codegen tests read these, so they all start from the same files.

- `todo-server/`: `examples/todo-server-rs`, one schema version, open permissions.
- `shared-todos/`: two versions joined by a migration (a rename and introduced columns), `ExistsRel`, `Inherits` and boolean policies, and enum, JSON-schema, array, indexed and latest-only tables. Its current tables have relations, a computed field and a transformed column.

Each directory holds `jazz.app.json` (schemas, migrations, codegen: what clients ship) and `jazz.permissions.json` (the current version's permissions and their bundle: what only backends read).

The Rust test `the_writer_reproduces_each_fixture_byte_for_byte` rebuilds every file and fails if one differs.
After an intended change, regenerate them:

```sh
JAZZ_UPDATE_FIXTURES=1 cargo test -p jazz-core --lib artifact
```

The format is specified in SPEC §10.10 (`crates/jazz/SPEC/10_lenses_migrations.md`).
