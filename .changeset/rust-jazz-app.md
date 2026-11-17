---
"jazz-tools": patch
---

**Rust: a typed client for a Jazz app, opened from its files.** The new `jazz-app-support` crate opens an app from its `jazz.app.json`. `jazz-tools generate` now also writes Rust bindings, `src/jazz_app.rs`, when a `Cargo.toml` sits beside `jazz.app.json`. It writes them from `jazz_codegen::rust`, and `jazz_codegen::rust::build()` generates them from a `build.rs` instead.

- **The bindings.** Per table there is a row struct, a new-row struct and an update struct, and a module of typed columns, relations and queries. A `Computed` trait declares the app's computed fields and transforms. `open(config)` returns a `Send + Clone` `jazz_app_support::Jazz` that can sit in an Axum router's state.
- **The API.** Typed `all`, `one`, `get`, `insert`, `update` and `delete`, transactions, and `subscribe`, which returns a `futures::Stream` of typed deltas.
- **Backends.** Opened as the app's backend (`Config::backend`), it publishes the app's permissions bundle at startup with the admin secret, as `createJazz` does. It then admits the user it acts as with the backend secret, and pins that session to the bundle.
- **Not supported yet.** Connecting to a server as an end user (JWT), includes and reads inside transactions are refused with "not supported yet".

`examples/todo-server-rs` uses the new crate: it no longer needs `JAZZ_BUNDLE`, `schema.ts` or the `jazz-tools schema compile` shell-out, and it takes `JAZZ_ADMIN_SECRET`.
