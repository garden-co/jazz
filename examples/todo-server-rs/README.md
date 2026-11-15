# todo-server-rs

An Axum REST + SSE service over a Jazz app, written in Rust with the `jazz-app` crate. It runs as the app's backend: SQLite storage, the native WebSocket transport, no Node, NAPI or WASM.

## What it demonstrates

- **The app is its files.** `jazz.app.json` declares the tables (and their history and migrations), and `jazz.permissions.json` declares who may do what. `src/jazz_app.rs` holds the typed bindings that `jazz-tools generate` generates from `jazz.app.json`. All three are committed, and there is no schema code to run.
- **Typed bindings.** `jazz_app::Todo`, `NewTodo` and `TodoUpdate`, plus `todos::query()`, `todos::done().eq(false)` and `todos::todos_via_parent().of(id)`.
- **A `Send + Clone` handle.** `jazz_app::open(config)` returns a `jazz_app::Jazz`. The handle lives directly in Axum's router state, while the client runs on a thread of its own.
- **Running as the app's backend**, as `createJazz` does:
  - On start it publishes the app's schema versions, migrations and permissions as a bundle, using the admin secret.
  - It vouches for the one user it acts as (`TODO_USER_ID`, named `jazz:backend`), using the backend secret.
  - Its session is pinned to that bundle.
- **CRUD** over `/todos`, backed by typed inserts, updates and deletes.
- **Server-Sent Events** on `/todos/live`: a subscription (a `futures::Stream` of typed deltas) is applied to a `Rows` list, and the whole list is sent on every change.

## Schema

From `jazz.app.json`:

- **projects**: name
- **todos**: title, done, description (optional), parent (a todo, optional), project (optional)

`jazz.permissions.json` lets everyone read and write todos. Account admission does not replace table permissions.

## Changing the schema

```bash
# Edit the tables in jazz.app.json, then:
npx jazz-tools migrations create --name <name>   # resolve any "TODO: …" it writes
npx jazz-tools generate                          # checks the files, regenerates src/jazz_app.rs
```

Commit `jazz.app.json` and `src/jazz_app.rs`. In CI, `npx jazz-tools generate --check` refuses stale files, and so does this example's `bindings_are_current` test. To generate the bindings at build time instead of committing them, call `jazz_codegen::rust::build()` from a `build.rs` and `include!` its output (see that function's docs).

## Running locally

```bash
# A Jazz server for the app, with its admin and backend secrets:
export JAZZ_APP_ID="<APP_UUID>"
export JAZZ_SERVER_URL="http://localhost:1625"
export JAZZ_ADMIN_SECRET="<ADMIN_SECRET>"
export JAZZ_BACKEND_SECRET="<BACKEND_SECRET>"
cargo run -p todo-server
```

On Ctrl-C or SIGTERM it stops accepting requests, ends the `/todos/live` streams and lets the requests in flight finish (for up to 10 seconds). Then it calls `jazz.shutdown()`. With a persistent store, call `shutdown` before the process exits, so that the calls already made finish and the store closes.

The service runs as one configured user. HTTP requests don't select separate Jazz identities, and its HTTP API has no sign-in of its own, so serve it only where your own infrastructure authenticates callers.

| Variable              | Default                                |
| --------------------- | -------------------------------------- |
| `JAZZ_APP_ID`         | hard-coded fallback id (see `main.rs`) |
| `JAZZ_SERVER_URL`     | `http://localhost:1625`                |
| `JAZZ_ADMIN_SECRET`   | required: publishes the bundle         |
| `JAZZ_BACKEND_SECRET` | required: admits the user it acts as   |
| `TODO_USER_ID`        | `todo-service`                         |
| `TODO_DATA_DIR`       | `./todo-data`                          |
| `TODO_PORT`           | `3000`                                 |

## API

| Route         | Method   | Description                       |
| ------------- | -------- | --------------------------------- |
| `/todos`      | `GET`    | List all todo items               |
| `/todos`      | `POST`   | Create new item                   |
| `/todos/:id`  | `GET`    | Get a single item                 |
| `/todos/:id`  | `PUT`    | Update item                       |
| `/todos/:id`  | `DELETE` | Delete item                       |
| `/todos/live` | `GET`    | SSE stream of full-list snapshots |
| `/health`     | `GET`    | Liveness probe                    |

## Tests

```bash
cargo test -p todo-server
```

The tests drive the real router:

- CRUD, persistence across restarts and the SSE stream run on a local app.
- Resync runs against an in-process Jazz server: one backend publishes the bundle and writes as Alice, and a second backend with an empty store reads the write as Bob.

The documentation copy in `examples/docs/todo-server-rs` (`todo-server-docs`) is the same service, plus the Rust snippets the docs include.
