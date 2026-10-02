# todo-server-rs

Axum REST + SSE service backed by the `jazz` Rust client, RocksDB storage and the native WebSocket transport. The database path uses no NAPI or WASM. Application startup exports the TypeScript schema through the Jazz CLI, which requires the repository's JavaScript dependencies and built packages.

## What it demonstrates

- Using `JazzClient::connect_with_native_transport` with persistent storage and a remote sync server.
- Enrolling an ordinary user with `JAZZ_JWT_TOKEN` through `/accounts/login-or-register` before connecting. The returned account supplies `AppContext.account_id`; neither backend nor admin credentials are used by the todo worker. Local-first tokens use `/accounts/found-local-first`.
- Loading a TypeScript schema from Rust by shelling out to the `jazz-tools` CLI (`schema export`) at startup &mdash; the schema is authored once in `schema.ts` and consumed both by the JS tooling and the Rust client.
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

# Use an existing Jazz server configured with this schema, its permissions,
# and a JWT verifier for your auth provider. Use the same app UUID and a
# valid ordinary-user JWT; do not put a backend/admin secret in JAZZ_JWT_TOKEN.
export JAZZ_APP_ID="<APP_UUID>"
export JAZZ_SERVER_URL="http://localhost:1625"
export JAZZ_JWT_TOKEN="<USER_JWT>"
cargo run --manifest-path examples/todo-server-rs/Cargo.toml

# The documentation copy has its own excluded manifest:
cargo run --manifest-path examples/docs/todo-server-rs/Cargo.toml
```

Both examples remain deliberately excluded from the root Cargo workspace. Build and test them with `--manifest-path`, not `-p` from the repository root. The todo service runs as one configured user; HTTP requests do not select separate Jazz identities.

Configurable via env vars (`JAZZ_JWT_TOKEN` is required):

| Variable          | Default                                 |
| ----------------- | --------------------------------------- |
| `JAZZ_APP_ID`     | hard-coded fallback id (see `main.rs`)  |
| `JAZZ_SERVER_URL` | `http://localhost:1625`                 |
| `JAZZ_JWT_TOKEN`  | required ordinary-user JWT              |
| `TODO_DATA_DIR`   | `./todo-data` (local data directory)    |
| `TODO_PORT`       | `3000`                                  |
| `JAZZ_TOOLS_BIN`  | `jazz-tools` (used for `schema export`) |

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

The focused smoke starts an isolated Jazz server, publishes the example's open policies as fixture setup, and connects two independent todo workers using the same ordinary-user JWT with `account_id: None` and no backend/admin credentials. One writes; the other retries enrolment and reads the row over native sync. This checks the worker path, not TypeScript schema export or application deployment. The full suites retain the CRUD, persistence, SSE and process-level resync assertions.
