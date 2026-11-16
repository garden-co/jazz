# todo-server-rs

An Axum REST + SSE service over a Jazz app, written in Rust with the `jazz-app-support` crate. It is a **backend client** of the app's TypeScript main backend (`createJazz`): it syncs through the main backend's `/jazz` with a machine credential (an OIDC token) that the main backend trusts through `trustWorkload`. It uses SQLite storage and the native WebSocket transport, with no Node, NAPI or WASM.

## What it demonstrates

- **The app is authored in `schema.ts`.** It declares the tables and relations (`s.defineSchema`) and who may do what (`definePermissions`). `npx jazz-tools generate` writes `jazz.app.json` (the tables, with their history and migrations), `jazz.permissions.json` and `src/jazz_app.rs`, the typed bindings. All of them are committed. The service reads only the generated files: nothing runs `schema.ts` at runtime, and there is no Node in the service.
- **Typed bindings.** `jazz_app::Todo`, `NewTodo` and `TodoUpdate`, plus `todos::query()`, `todos::done().eq(false)` and `todos::todos_via_parent().of(id)`.
- **A `Send + Clone` handle.** `jazz_app::open(config)` returns a `jazz_app_support::Jazz`. The handle lives directly in Axum's router state, while the client runs on a thread of its own.
- **Running as a backend client**, as TypeScript's `createJazzClient` does:
  - It connects to the main backend's `/jazz` with `Config::credential`.
  - The main backend decides what the credential is trusted as. A **scoped** client acts as one user, under the app's permissions. A **trusted** client acts as the system.
  - On shutdown, `jazz.close(CLOSE_TIMEOUT)` waits for writes to reach the main backend.
- **CRUD** over `/todos`, backed by typed inserts, updates and deletes.
- **Server-Sent Events** on `/todos/live`: a subscription (a `futures::Stream` of typed deltas) is applied to a `Rows` list, and the whole list is sent on every change.

## Schema

From `schema.ts`:

- **projects**: name
- **todos**: title, done, description (optional), parent (a todo, optional), project (optional)

Its permissions (`jazz.permissions.json` once generated) let everyone read and write todos. Account admission does not replace table permissions.

## Changing the schema

```bash
# Edit schema.ts, then:
npx jazz-tools migrations create --name <name>   # resolve any "TODO: …" it writes
npx jazz-tools generate                          # writes jazz.app.json, jazz.permissions.json and src/jazz_app.rs
```

`npx jazz-tools generate` runs `schema.ts` with its own copy of `jazz-tools`, so the crate needs no `package.json`. It finds `Cargo.toml` beside `schema.ts` and writes the Rust bindings. Run it with `npx`: the standalone `jazz-tools` binary refuses to write beside a `schema.ts`, though its `generate --check` still checks the committed files and bindings (not that they are fresh from the TypeScript). Commit `schema.ts`, `migrations/`, `jazz.app.json`, `jazz.permissions.json` and `src/jazz_app.rs`. Don't edit the generated files. In CI, `npx jazz-tools generate --check` refuses stale files, and so does this example's `bindings_are_current` test. To generate the bindings at build time instead of committing them, call `jazz_codegen::rust::build()` from a `build.rs` and `include!` its output (see that function's docs). It reads the generated `jazz.app.json`, so run `npx jazz-tools generate` after editing `schema.ts` either way.

## Running locally

```bash
# The app's TypeScript main backend (createJazz) trusts this service's machine
# credential in backendClients, e.g. scoped as one user:
#   trustWorkload({ issuer, audience, claims: { sub: "todo-service" }, as: () => ({ userId: "todo-service" }) })
export JAZZ_SERVER_URL="https://todo.example/jazz"   # the main backend's /jazz
export JAZZ_APP_ID="<jazz.appId>"
export JAZZ_TOKEN_FILE=/var/run/secrets/tokens/jazz  # or JAZZ_TOKEN / JAZZ_TOKEN_COMMAND
cargo run -p todo-server
```

On Ctrl-C or SIGTERM it stops accepting requests, ends the `/todos/live` streams and lets the requests in flight finish (for up to 10 seconds). Then it calls `jazz.close(CLOSE_TIMEOUT)`, which waits up to 10 seconds for writes to reach the main backend. With a persistent store, call `shutdown` before the process exits, so that the calls already made finish and the store closes.

The service runs as the one identity its credential is trusted as. HTTP requests don't select separate Jazz identities, and its HTTP API has no sign-in of its own, so serve it only where your own infrastructure authenticates callers.

| Variable             | Default                                               |
| -------------------- | ----------------------------------------------------- |
| `JAZZ_SERVER_URL`    | required: the main backend's Jazz endpoint (`…/jazz`) |
| `JAZZ_APP_ID`        | required: the main backend's app id (`jazz.appId`)    |
| `JAZZ_TOKEN`         | the machine credential (an OIDC token), or            |
| `JAZZ_TOKEN_FILE`    | a file holding it (read again for every token), or    |
| `JAZZ_TOKEN_COMMAND` | a shell command printing it (run for every token)     |
| `TODO_DATA_DIR`      | `./todo-data`                                         |
| `TODO_PORT`          | `3000`                                                |

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

[`examples/rust-backend-clients`](../rust-backend-clients) runs the service against a real TypeScript `createJazz`, as a scoped backend client. CI runs it with the TypeScript tests. The Rust tests below run on their own:

```bash
cargo test -p todo-server
```

The tests drive the real router:

- CRUD, persistence across restarts and the SSE stream run on a local app.
- Resync runs against an in-process Jazz server. Two clients in main-backend mode stand in for the TypeScript main backend there: one publishes the bundle and writes as Alice, and a second with an empty store reads the write as Bob.

The documentation copy in `examples/docs/todo-server-rs` (`todo-server-docs`) is the same service, plus the Rust snippets the docs include.
