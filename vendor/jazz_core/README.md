# Jazz Core

Jazz Core is a local-first distributed database core: **jazz** provides the
distributed database semantics and public API, built on **groove**, an embedded
incremental view maintenance engine over RocksDB. jazz lowers schemas, queries,
policy, sync views, and history-aware reads onto groove so local storage,
indexes, prepared query graphs, and maintained views share one underlying
execution path.

## Core Crates

- **groove**: embedded incremental view maintenance engine, OLTP row store,
  typed record encoding, indexes, subscriptions, prepared query graphs, and the
  `OrderedKvStorage` storage seam. RocksDB is the normal durable storage path.
- **jazz**: local-first distributed database layer with transactions, history,
  fates, sync, read/write policy, schema-version catalogue data, branches,
  time-travel reads, and the public `Db` facade.

## Additional Workspace Crates

- **jazz-sim**: deterministic simulation and scenario benchmark crate for
  exercising distributed behavior under controlled delivery and replay.
- **jazz-server**: Rust sync server for local development and integration
  smoke checks, with reconnect/resume, drain, health, metrics, HTTP frame
  diagnostics, and a WebSocket byte-frame listener.
- **jazz-wasm**: WebAssembly binding crate. It should wrap core `Db`/`Node`
  objects directly and use core payloads such as row batches and subscription
  chunks, encoded with postcard where a byte boundary is useful.
- **future jazz-napi**: native Node bindings should be a sibling binding crate
  over the same direct core-object binding shape as WASM.

## Getting Started

Run the broad local verification stack from the repository root:

```sh
cargo fmt --all --check
cargo test -p jazz
cargo test -p groove
cargo test -p jazz-server
cargo test -p jazz-sim --test scenario_smoke
cargo test --doc -p groove
cargo test --doc -p jazz
scripts/test_wasm_bindings.sh
cargo clippy --workspace --all-targets -- -D warnings
```

Doctests and `scripts/test_wasm_bindings.sh` are most important when public
examples, docs, binding DTOs, or TypeScript/WASM example code changes.

To run the `jazz-server` dry-run check without binding sockets, opening
storage, or starting a runtime:

```sh
cargo run -p jazz-server -- dry-run
```

The current CLI shape is intentionally small and process-oriented:

```sh
cargo run -p jazz-server -- dry-run \
  --listen 127.0.0.1:1625 \
  --data-dir ./data \
  --websocket-path /sync \
  --auth-static-bearer "$JAZZ_ADMIN_SECRET"

cargo run -p jazz-server -- serve <schema-postcard-hex> \
  --listen 127.0.0.1:1625 \
  --data-dir ./data
```

The same shape can be configured with `JAZZ_SERVER_LISTEN`,
`JAZZ_SERVER_PORT`, `JAZZ_SERVER_DATA_DIR`, `JAZZ_SERVER_IN_MEMORY`,
`JAZZ_SERVER_WEBSOCKET_PATH`, `JAZZ_SERVER_AUTH_STATIC_BEARER`, or the
upstream-compatible `JAZZ_ADMIN_SECRET` fallback. `--in-memory` overrides a
durable data-dir setting for volatile local runs.

`cargo test -p jazz-server` also exercises the current HTTP and WebSocket
listeners around `InMemoryServerShell`. The HTTP smoke check covers
health, metrics, session admission, and newline-separated hex frame request
plumbing; the WebSocket smoke check sends postcard batches of raw ABI
`WireFrame` bytes as binary messages and proves sync between two clients.

## Examples

- [jazz todo](jazz/examples/todo.rs): shows the jazz `Db` facade with a small
  todo workflow.
  ```sh
  cargo run --example todo -p jazz
  ```
- [jazz transactions](jazz/examples/transactions.rs): shows `mergeable_tx`
  batching and `exclusive_tx` conflict rejection.
  ```sh
  cargo run --example transactions -p jazz
  ```
- [jazz permissions](jazz/examples/permissions.rs): shows dry-run permission
  previews and Core-attributed writes.
  ```sh
  cargo run --example permissions -p jazz
  ```
- [groove quickstart](groove/examples/quickstart.rs): shows basic groove
  `Database` setup, writes, and queries.
  ```sh
  cargo run --example quickstart -p groove
  ```
- [jazz-tools alpha example](examples/jazz-tools/README.md):
  Node example for alpha-shaped todo, bool/contains reads, shared-with-me access,
  identity update dry-runs, chat-lite policy, snapshot-backed HTTP restart, and
  a spawned Rust `jazz-server` sync boundary with opt-in durable WebSocket
  restart gates.
  ```sh
  cd examples/jazz-tools && npm test
  ```
- [TS WASM browser example](examples/browser-wasm/README.md):
  Vite scaffold with an alpha-like main-thread/Web Worker split and headless
  browser smoke check, including OPFS-backed reload persistence through
  `storageOpen("browser", namespace)`.
  ```sh
  cd examples/browser-wasm && npm test
  ```

## Documentation

The design and contract for each crate live in a single per-crate spec:

- **[jazz/SPEC/](jazz/SPEC/)** — data model, transactions, history & merging,
  reads, queries, authorization, the sync protocol, topology & the edge tier,
  lenses/migrations, branches, large values, the `Db` API, lowering to groove,
  sharding, maintained subscription views, integrability; guidance appendices
  (implementation discipline, benchmarks, performance, testing, glossary); and
  [`INVARIANTS.md`](jazz/SPEC/INVARIANTS.md).
- **[groove/SPEC/](groove/SPEC/)** — the IVM engine: storage model, queries &
  operators, incremental maintenance, prepared shapes, recursion, correctness;
  guidance appendices; and [`INVARIANTS.md`](groove/SPEC/INVARIANTS.md).

Start at each spec's `1_intro.md`. Operational and in-flight material (benchmark
specifics, perf receipts, slice plans) lives inside the relevant chapter's
`In flight` section — one home, settling upward into the normative body over time.
