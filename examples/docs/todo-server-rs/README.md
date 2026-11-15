# todo-server-rs (docs copy)

The documentation copy of [`examples/todo-server-rs`](../../todo-server-rs): the same Axum REST + SSE service over a Jazz app, run as the app's backend with the `jazz-app` crate, plus the Rust snippets the docs include. It is the workspace package `todo-server-docs`.

## Files

- `jazz.app.json`: the tables (and their history and migrations).
- `jazz.permissions.json`: who may do what. The service publishes it as a bundle on start.
- `src/jazz_app.rs`: the typed bindings `jazz-tools generate` generates from `jazz.app.json`. Do not edit it.
- `src/main.rs`: opens the app with `jazz_app::open(config)` as a backend and serves the router.
- `src/routes.rs`: CRUD over `/todos` and the `/todos/live` SSE stream.
- `src/docs_snippets.rs`: the snippets. The docs include each `// #region <name>` by name, so keep the names when you edit them.

Most snippets use the typed API: `jazz.all`, `one`, `get`, `insert`, `update`, `delete`, `transaction` and `subscribe`, with `todos::query()`, `todos::done().eq(false)`, `todos::title().contains("milk")`, `order_by(jazz_app::created_at(), …)`, `projects::todos().of(id)`, `NewTodo::new(title, done)` and `TodoUpdate { parent: Some(None), .. }`. What `jazz-app` doesn't cover yet uses the low-level `JazzClient`, either directly or through `Jazz::with_client`:

- per-request sessions (`for_session`): the typed handle acts as the backend's one user;
- includes, `select`, recursive queries (`gather`);
- files (large values);
- serializable transactions that read.

## Schema

From `jazz.app.json`:

- **projects**: name; `todos` (its todos)
- **todos**: title, done, priority (optional), description (optional), parent (a todo, optional), project (optional); `children` (its sub-todos)

## Changing the schema

```bash
# Edit the tables in jazz.app.json, then:
npx jazz-tools migrations create --name <name>   # resolve any "TODO: …" it writes
npx jazz-tools generate --lang rust              # checks the files, regenerates src/jazz_app.rs
```

Commit `jazz.app.json` and `src/jazz_app.rs`. The `bindings_are_current` test refuses stale bindings.

## Running locally

```bash
# A Jazz server for the app, with its admin and backend secrets:
export JAZZ_APP_ID="<APP_UUID>"
export JAZZ_SERVER_URL="http://localhost:1625"
export JAZZ_ADMIN_SECRET="<ADMIN_SECRET>"
export JAZZ_BACKEND_SECRET="<BACKEND_SECRET>"
cargo run -p todo-server-docs
```

| Variable              | Default                              |
| --------------------- | ------------------------------------ |
| `JAZZ_APP_ID`         | `todo-app` (an id derived from it)   |
| `JAZZ_SERVER_URL`     | `http://localhost:1625`              |
| `JAZZ_ADMIN_SECRET`   | required: publishes the bundle       |
| `JAZZ_BACKEND_SECRET` | required: admits the user it acts as |
| `TODO_USER_ID`        | `todo-service`                       |
| `TODO_DATA_DIR`       | `./todo-data`                        |
| `TODO_PORT`           | `3000`                               |

On Ctrl-C or SIGTERM it stops accepting requests, ends the live streams, lets the requests in flight finish (for up to 10 seconds) and calls `jazz.shutdown()`. The HTTP API has no sign-in of its own: serve it only where your infrastructure authenticates callers.

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
cargo test -p todo-server-docs
```

They drive the real router: CRUD, persistence across restarts, the SSE stream and its end on shutdown run on a local app; resync runs against an in-process Jazz server, with one backend writing as Alice and a second one, with an empty store, reading the write as Bob. `bindings_are_current` checks `src/jazz_app.rs` against `jazz.app.json`.
