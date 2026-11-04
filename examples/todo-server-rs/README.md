# todo-server-rs

Axum REST + SSE service backed by the `jazz` Rust client, SQLite storage and the native WebSocket transport. The database path uses no NAPI or WASM. Application startup exports the TypeScript schema through the Jazz CLI, which requires the repository's JavaScript dependencies and built packages.

## What it demonstrates

- Using `JazzClient::connect_with_native_transport` with persistent storage and a remote sync server.
- Running as the app's backend: the service holds the sync server's backend secret (`JAZZ_BACKEND_SECRET`) and acts as one user of the app (`TODO_USER_ID`), named `jazz:app:<appId>` as `createJazz` names its users. Before connecting it vouches for that user at `/admin/accounts/login-or-register`, and the returned account goes into its `backend_session`. That session is pinned to a published permissions bundle (`JAZZ_BUNDLE`), as `createJazz` pins the sessions it states: a core governs a stated session only by its bundle. It never holds the admin secret.
- Loading a TypeScript schema from Rust by shelling out to the `jazz-tools` CLI (`schema compile`) at startup &mdash; the schema is authored once in `schema.ts` and consumed both by the JS tooling and the Rust client.
- CRUD over `/todos` (`GET`, `POST`, `PUT /:id`, `DELETE /:id`) backed by Jazz inserts / updates / deletes.
- Server-Sent Events on `/todos/live` broadcasting the full todo list whenever it changes, via a `tokio::sync::broadcast` channel.
- `mimalloc` as the native example's global allocator.

## Schema

Defined in `schema.ts` (same DSL as every other example):

- **projects** &mdash; name
- **todos** &mdash; title, done, description, parent (self-ref, optional), project (optional)

Permissions in `permissions.ts` allow todo reads and writes for everyone. Publish them alongside the schema on the sync server. Account enrolment does not replace table permissions.

## Running locally

```bash
# Run from the repository root, after preparing the JavaScript schema tooling:
pnpm install
pnpm build:core
cargo build -p jazz-cli --bin jazz-tools
export JAZZ_TOOLS_BIN="$PWD/target/debug/jazz-tools"

# Use an existing Jazz server where the app's backend published this
# schema's permissions as a bundle. Use the same app UUID, that server's
# backend secret and the bundle's id.
export JAZZ_APP_ID="<APP_UUID>"
export JAZZ_SERVER_URL="http://localhost:1625"
export JAZZ_BACKEND_SECRET="<BACKEND_SECRET>"
export JAZZ_BUNDLE="<BUNDLE_ID>"
cargo run --manifest-path examples/todo-server-rs/Cargo.toml

# The documentation copy has its own excluded manifest:
cargo run --manifest-path examples/docs/todo-server-rs/Cargo.toml
```

Both examples remain deliberately excluded from the root Cargo workspace. Build and test them with `--manifest-path`, not `-p` from the repository root. The todo service runs as one configured user; HTTP requests do not select separate Jazz identities. Its HTTP API has no sign-in of its own, so serve it only where your own infrastructure authenticates callers.

Configurable via env vars (`JAZZ_BACKEND_SECRET` and `JAZZ_BUNDLE` are required):

| Variable              | Default                                                   |
| --------------------- | --------------------------------------------------------- |
| `JAZZ_APP_ID`         | hard-coded fallback id (see `main.rs`)                    |
| `JAZZ_SERVER_URL`     | `http://localhost:1625`                                   |
| `JAZZ_BACKEND_SECRET` | required: the sync server's backend secret                |
| `JAZZ_BUNDLE`         | required: the permissions bundle its session is pinned to |
| `TODO_USER_ID`        | `todo-service` (the user it acts as)                      |
| `TODO_DATA_DIR`       | `./todo-data` (local data directory)                      |
| `TODO_PORT`           | `3000`                                                    |
| `JAZZ_TOOLS_BIN`      | `jazz-tools` (used for `schema compile`)                  |

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
# Focused native smoke: no external server, CLI or JavaScript build required.
cargo test --manifest-path examples/todo-server-rs/Cargo.toml --test native_user
cargo test --manifest-path examples/docs/todo-server-rs/Cargo.toml --test native_user

# Compile every target in each independent example:
cargo test --manifest-path examples/todo-server-rs/Cargo.toml --all-targets --no-run
cargo test --manifest-path examples/docs/todo-server-rs/Cargo.toml --all-targets --no-run

# Full example suites also need the CLI for the process-level resync test:
cargo build -p jazz-cli --bin jazz-tools
export JAZZ_TOOLS_BIN="$PWD/target/debug/jazz-tools"
cargo test --manifest-path examples/todo-server-rs/Cargo.toml --all-targets
cargo test --manifest-path examples/docs/todo-server-rs/Cargo.toml --all-targets
```

The focused smoke starts an isolated Jazz server, publishes the example's open policies as fixture setup, and connects two independent todo workers that both act as Alice with the backend secret (no admin secret). Each resolves her one account; one writes and the other reads the row over native sync. This checks the worker path, not TypeScript schema export or application deployment. The full suites retain the CRUD, persistence, SSE and process-level resync assertions.
