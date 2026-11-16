# todo-server-rs (docs copy)

> [!WARNING]
> **Experimental: client only; backends are TypeScript.** See [Language Support](https://jazz.tools/docs/reference/language-support#jazz-environment-rust).

The documentation copy of [`examples/todo-server-rs`](../../todo-server-rs): the same Axum REST + SSE service over a Jazz app, run as a backend client of the app's TypeScript main backend with the `jazz-app-support` crate, plus the Rust snippets the docs include. It is the workspace package `todo-server-docs`.

## Files

- `schema.ts`: the app's tables and relations (`s.defineSchema`) and its permissions (`definePermissions`). This is the file you edit.
- `migrations/`: the migrations between schema versions, and their snapshots, written by `npx jazz-tools migrations create`.
- `jazz.app.json`: generated from `schema.ts` and `migrations/` by `npx jazz-tools generate`: the tables, with their history and migrations. The service reads it at runtime. Do not edit it.
- `jazz.permissions.json`: generated from the permissions in `schema.ts`. The main backend serves it, and this service opens with it as a backend client. Do not edit it.
- `src/jazz_app.rs`: the typed bindings, also written by `npx jazz-tools generate`. Do not edit it.
- `src/main.rs`: opens the app as a backend client, with `Jazz::open(AppFiles::new(jazz_app::APP, None).with_permissions(…), config)`, and serves the router.
- `src/credential.rs`: the machine credential, from `JAZZ_TOKEN`, `JAZZ_TOKEN_FILE` or `JAZZ_TOKEN_COMMAND`.
- `src/routes.rs`: CRUD over `/todos` and the `/todos/live` SSE stream.
- `src/docs_snippets.rs`: the snippets. The docs include each `// #region <name>` by name, so keep the names when you edit them.

Most snippets use the typed API: `jazz.all`, `one`, `get`, `insert`, `update`, `delete`, `transaction` and `subscribe`, with `todos::query()`, `todos::done().eq(false)`, `todos::title().contains("milk")`, `order_by(jazz_app::created_at(), …)`, `projects::todos().of(id)`, `NewTodo::new(title, done)` and `TodoUpdate { parent: Some(None), .. }`. The includes, `select` and files snippets still use the low-level `JazzClient`, either directly or through `Jazz::with_client`, though `jazz-app-support` covers them too now: includes (`Query::include`), `select` (`Partial` rows), `hop_to`, `gather` and `union`, `history()`, large values (`LargeColumn`, streaming inserts, and partial reads with `Query::part` and `ValuePart`) and serializable transactions that read.

## Schema

From `schema.ts`:

- **projects**: name; `todos` (its todos)
- **todos**: title, done, priority (optional), description (optional), parent (a todo, optional), project (optional); `children` (its sub-todos)

## Changing the schema

```bash
# Edit schema.ts, then:
npx jazz-tools migrations create --name <name>   # resolve any "TODO: …" it writes
npx jazz-tools generate                          # writes jazz.app.json, jazz.permissions.json and src/jazz_app.rs
```

`generate` finds `Cargo.toml` beside `schema.ts` and writes the Rust bindings; the crate needs no `package.json`. Run it with `npx`: the standalone `jazz-tools` binary refuses to write beside a `schema.ts`, though its `generate --check` still checks the committed files and bindings (not that they are fresh from the TypeScript). Commit `schema.ts`, `migrations/` and the generated files. The `bindings_are_current` test refuses stale bindings, and `npx jazz-tools generate --check` refuses stale files in CI.

## Running locally

```bash
# The app's TypeScript main backend (createJazz) trusts this service's machine
# credential in backendClients, e.g. scoped as one user:
#   trustWorkload({ issuer, audience, claims: { sub: "todo-service" }, as: () => ({ userId: "todo-service" }) })
export JAZZ_SERVER_URL="https://todo.example/jazz"   # the main backend's /jazz
export JAZZ_APP_ID="<jazz.appId>"
export JAZZ_TOKEN_FILE=/var/run/secrets/tokens/jazz  # or JAZZ_TOKEN / JAZZ_TOKEN_COMMAND
cargo run -p todo-server-docs
```

| Variable             | Default                                               |
| -------------------- | ----------------------------------------------------- |
| `JAZZ_SERVER_URL`    | required: the main backend's Jazz endpoint (`…/jazz`) |
| `JAZZ_APP_ID`        | required: the main backend's app id (`jazz.appId`)    |
| `JAZZ_TOKEN`         | the machine credential (an OIDC token), or            |
| `JAZZ_TOKEN_FILE`    | a file holding it (read again for every token), or    |
| `JAZZ_TOKEN_COMMAND` | a shell command printing it (run for every token)     |
| `TODO_DATA_DIR`      | `./todo-data`                                         |
| `TODO_PORT`          | `3000`                                                |

On Ctrl-C or SIGTERM it stops accepting requests, ends the live streams, lets the requests in flight finish (for up to 10 seconds) and calls `jazz.close(CLOSE_TIMEOUT)`, which waits up to 10 seconds for writes to reach the main backend. The HTTP API has no sign-in of its own: serve it only where your infrastructure authenticates callers.

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

[`examples/rust-backend-clients`](../../rust-backend-clients) runs the service against a real TypeScript `createJazz`, as a scoped backend client. CI runs it with the TypeScript tests. The Rust tests below run on their own:

```bash
cargo test -p todo-server-docs
```

They drive the real router: CRUD, persistence across restarts, the SSE stream and its end on shutdown run on a local app; resync runs against an in-process Jazz server, where two clients in main-backend mode stand in for the TypeScript main backend: one writes as Alice, and a second, with an empty store, reads the write as Bob. `bindings_are_current` checks `src/jazz_app.rs` against `jazz.app.json`.
