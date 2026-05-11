# todo-server-rs

Axum REST + SSE service backed by Jazz, using the `jazz-tools` Rust crate directly &mdash; no NAPI, no Node, no WASM. The point of the example is to show that the same database that powers the browser and Node examples can be embedded straight into a Rust process.

## What it demonstrates

- Using `jazz-tools` as a Rust crate (`JazzClient::connect`) with persistent storage and a remote sync server.
- Loading a TypeScript schema from Rust by shelling out to the `jazz-tools` CLI (`schema export`) at startup &mdash; the schema is authored once in `schema.ts` and consumed both by the JS tooling and the Rust client.
- CRUD over `/todos` (`GET`, `POST`, `PUT /:id`, `DELETE /:id`) backed by Jazz inserts / updates / deletes.
- Server-Sent Events on `/updates` broadcasting the full todo list whenever it changes, via a `tokio::sync::broadcast` channel.
- `mimalloc` swapped in as the global allocator for a meaningful throughput win on the Rust-side allocation-heavy paths (query / insert / observer).

## Schema

Defined in `schema.ts` (same DSL as every other example):

- **projects** &mdash; name
- **todos** &mdash; title, done, description, parent (self-ref, optional), project (optional)

Permissions in `permissions.ts` are wide open (`allowRead`/`Insert`/`Update`/`Delete` for everyone). This example is about the Rust-side wiring, not row-level security &mdash; see `todo-server-ts` for the permissions / session story.

## Running locally

```bash
# 1. Create an app and start the Jazz sync server
jazz-tools create app --name todo-app
jazz-tools server <APP_ID> --port 1625

# 2. Run the Rust backend
cargo run -p todo-server
```

Configurable via env vars (all optional):

| Variable          | Default                                 |
| ----------------- | --------------------------------------- |
| `JAZZ_APP_ID`     | hard-coded fallback id (see `main.rs`)  |
| `JAZZ_SERVER_URL` | `http://localhost:1625`                 |
| `TODO_DATA_DIR`   | `./todo-data` (Fjall storage location)  |
| `TODO_PORT`       | `3000`                                  |
| `JAZZ_TOOLS_BIN`  | `jazz-tools` (used for `schema export`) |

## API

| Route        | Method   | Description                       |
| ------------ | -------- | --------------------------------- |
| `/todos`     | `GET`    | List all todo items               |
| `/todos`     | `POST`   | Create new item                   |
| `/todos/:id` | `GET`    | Get a single item                 |
| `/todos/:id` | `PUT`    | Update item                       |
| `/todos/:id` | `DELETE` | Delete item                       |
| `/updates`   | `GET`    | SSE stream of full-list snapshots |
| `/health`    | `GET`    | Liveness probe                    |

## Tests

```bash
cargo test -p todo-server
```

Tests spin up the axum app against an in-process Jazz client and exercise the CRUD lifecycle and the SSE stream end-to-end.
