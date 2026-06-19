# Yew Storage Benchmark Harness Design

Date: 2026-06-19

## Context

The current storage benchmark compares `opfs-btree` and SQLite in the browser over
OPFS, using identical `.kv` and `.ops` workloads and checksum parity as the
correctness gate. The engine loops are already Rust-in-wasm, but the primary
harness is a Node/Playwright script that builds an inline HTML page and one
JavaScript worker that imports both engines.

The replacement should make the harness visible and easy to work with from Rust.
Yew should be the primary browser harness, while Node remains only as the
headless launcher and result printer for automation.

## Goals

- Replace `wasm-bench/run-compare.cjs` as the primary comparison runner.
- Use a Yew app for benchmark orchestration, status, parity checks, and result
  rendering.
- Use two separate full Rust workers, one per engine, spawned through
  `gloo-worker`.
- Keep a single CLI command that builds and runs the benchmark headlessly.
- Preserve the existing benchmark methodology: identical workloads, OPFS-backed
  browser execution, and non-negotiable checksum parity.

## Non-Goals

- Add statistical repetition, warmup, medians, or percentiles.
- Change workload generation, dataset semantics, or the `.kv` / `.ops` formats.
- Tune SQLite or `opfs-btree`.
- Merge the two engine wasm builds into one artifact.
- Add a broad dashboard or polished product UI.
- Add JavaScript worker shims or bootstraps as a fallback.

## Architecture

Add a benchmark-local Yew/Trunk harness under:

```text
crates/opfs-btree/wasm-bench/harness/
```

The Yew app is the primary benchmark runner. It renders profile selection, run
status, checksum/parity state, and result tables. It also exposes a small
automation result on `window` so the Node launcher can wait for completion and
print results.

The app spawns two dedicated Rust workers through `gloo-worker`:

- `BtreeWorker`: fetches the selected `.kv` and `.ops` files, runs
  `opfs-btree`'s Rust benchmark path, and returns typed results.
- `SqliteWorker`: fetches the same `.kv` and `.ops` files, runs the SQLite
  Rust benchmark path, and returns typed results.

The Yew app coordinates the workers per profile, compares top-level checksums
when both engines finish, and fails the run if parity diverges. OPFS work stays
inside workers, where sync access handles are available.

The Node launcher remains intentionally small. It serves the built Trunk output,
launches Chromium, opens the harness with query parameters, waits for the
exported completion state, prints the result table or JSON, and exits non-zero
on failure.

If `gloo-worker` cannot support this two-worker Trunk/Yew setup cleanly, that is
a blocker to surface. The design does not include JavaScript worker bootstraps as
a fallback.

## Components

- `wasm-bench/harness/Cargo.toml`: benchmark-local Yew app crate. Expected core
  dependencies include `yew`, `gloo-worker`, `gloo-net`, `wasm-bindgen`,
  `web-sys`, `serde`, and a small shared result type module.
- `wasm-bench/harness/index.html`: Trunk entrypoint.
- `wasm-bench/harness/src/main.rs`: Yew app state machine, UI, worker
  coordination, checksum comparison, and automation export.
- `wasm-bench/harness/src/types.rs`: shared message and result types used by the
  app and both workers.
- `wasm-bench/harness/src/btree_worker.rs`: `gloo-worker` implementation for the
  `opfs-btree` engine.
- `wasm-bench/harness/src/sqlite_worker.rs`: `gloo-worker` implementation for the
  SQLite engine.
- `wasm-bench/harness/public/data/`: generated `.kv` and `.ops` files served by
  Trunk.

Package scripts should make the intended flow obvious:

- `bench:compare:data`: generate benchmark data.
- `bench:harness:build`: build the Yew/Trunk harness and Rust workers.
- `bench:compare`: generate data, build required wasm artifacts, build the Yew
  harness, run the Node launcher, and print results.
- `bench:compare:open`: serve the Yew harness for manual browser inspection.

The old `run-compare.cjs` comparison script should be removed or replaced by the
tiny launcher so there is one obvious path.

## Data Flow

1. `bench:compare` regenerates `.kv` and `.ops`, builds the required wasm
   artifacts, builds the Yew harness, and starts the Node launcher.
2. The launcher serves the Trunk output and opens a URL such as
   `/?profiles=objects,wikipedia&autorun=1`.
3. Yew parses the profile list and starts a run when `autorun=1`.
4. For each profile, Yew dispatches the same typed request to `BtreeWorker` and
   `SqliteWorker`.
5. Each worker fetches `/data/<profile>.kv` and `/data/<profile>.ops`, runs its
   engine, and returns either an engine result or a plain error.
6. Yew waits for both engine results for the profile.
7. Yew compares the top-level checksums. Matching checksums produce result rows;
   mismatch fails the whole run.
8. When all profiles complete or the first hard failure occurs, Yew writes a
   final automation result to `window`.
9. The launcher reads that result, prints a table or JSON, closes Chromium, and
   exits with the appropriate status code.

## Error Handling

Errors should be plain and early. The benchmark run fails on:

- missing generated data,
- failed dataset fetch,
- failed worker spawn or worker runtime error,
- engine exception,
- malformed result payload,
- checksum mismatch.

The checksum gate must not be weakened. A mismatch means the engines did not
perform identical work, so the result is invalid.

## Testing And Verification

Verification is command-oriented and black-box where practical:

- `cargo fmt --check`
- `cargo check --manifest-path crates/opfs-btree/wasm-bench/harness/Cargo.toml --target wasm32-unknown-unknown`
- `pnpm --dir crates/opfs-btree run bench:compare`

The end-to-end command is the important proof: it exercises Trunk, Yew,
`gloo-worker`, both Rust workers, OPFS browser execution, checksum parity,
headless automation, and result printing.

Existing tests should not be rewritten as part of this harness replacement. If
existing tests fail because of the broader benchmark branch state, that should be
reported separately instead of papered over.
