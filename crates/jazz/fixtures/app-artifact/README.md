# `jazz.app.json` fixtures

Canonical `jazz.app.json` files written by `jazz::tools::deployment::artifact::write`.
Every language's codegen tests read these, so they all start from the same files.

- `todo-server.json`: `examples/todo-server-rs`, one schema version, open permissions.
- `shared-todos.json`: two versions joined by a migration (a rename and introduced columns), `ExistsRel`, `Inherits` and boolean policies, and enum-payload, JSON-schema, array, indexed and latest-only tables. Its codegen section has relations, `ts_only` columns and a slice.

The Rust test `the_writer_reproduces_each_fixture_byte_for_byte` rebuilds both files and fails if they differ.
After an intended change, regenerate them:

```sh
JAZZ_UPDATE_FIXTURES=1 cargo test -p jazz --lib artifact
```

The format itself is documented on the `artifact` module (`crates/jazz/src/tools/deployment/artifact.rs`).
