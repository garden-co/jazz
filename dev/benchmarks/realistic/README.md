# Realistic E2E Benchmarks

Shared benchmark definitions for the realistic, scenario-driven benchmark suite.

## Files

- `schema/project_board.schema.json`: canonical collaborative app schema
- `profiles/s.json`: smoke/team-dev profile (`S`)
- `profiles/m.json`: medium profile (`M`)
- `scenarios/w1_interactive.json`: read-heavy interactive session
- `scenarios/w3_offline_reconnect.json`: offline writes then reconnect
- `scenarios/w4_cold_start.json`: reopen and first-query latency
- `scenarios/b1_server_crud_sustained.json`: sustained insert/update/delete throughput
- `scenarios/b2_server_reads_sustained.json`: sustained load request throughput
- `scenarios/b3_server_cold_load_large.json`: cold reopen/query with larger seeded data
- `scenarios/b4_server_fanout_updates.json`: update fanout delivery to many subscribers
- `scenarios/b5_server_permission_recursive.json`: recursive-permission read/update mix
- `scenarios/b6_server_hotspot_history.json`: deep-history hotspot updates + storage delta
- `scenarios/r8_many_branches.json`: many linked branches on a single object

## Native Runner (RocksDB)

Run from workspace root:

```bash
RUST_LOG=warn cargo run -p jazz-tools --features client,rocksdb --example realistic_bench -- \
  --profile dev/benchmarks/realistic/profiles/s.json \
  --scenario dev/benchmarks/realistic/scenarios/w1_interactive.json
```

`W3` requires a running server and `--server-url`:

```bash
RUST_LOG=warn cargo run -p jazz-tools --features client,rocksdb --example realistic_bench -- \
  --profile dev/benchmarks/realistic/profiles/s.json \
  --scenario dev/benchmarks/realistic/scenarios/w3_offline_reconnect.json \
  --server-url http://127.0.0.1:1625
```

## Rust Criterion (active direct-core baseline)

Run the local realistic benchmark suite:

```bash
cargo bench -p jazz-tools --features rocksdb --bench realistic_phase1_direct
```

It currently hard-codes the S profile inside
`crates/jazz-tools/benches/realistic_phase1_direct.rs` and runs the active
direct-core ports of selected realistic scenarios.

Current topology coverage:

- direct CRUD: `realistic_phase1_direct/r1_crud`
- direct reads: `realistic_phase1_direct/r2_reads`
- persisted cold-load (`M1_rocksdb`): `realistic_phase1_direct/r3_rocksdb_cold_load` (requires `--features rocksdb`)
- hot task history with multiple subscriptions: `realistic_phase1_direct/r4_hot_task_history`
- subscribed write path: `realistic_phase1_direct/r9_subscribed_write`
- direct writer/server/reader sync fanout: `realistic_phase1_direct/r10_direct_sync_fanout`
- byte-wire reconnect/resume canary: `realistic_phase1_direct/r11_byte_wire_resume`
- recursive permission read/subscription visibility: `realistic_phase1_direct/r12_recursive_permissions`

Run only the cold-load benchmark:

```bash
cargo bench -p jazz-tools --features rocksdb --bench realistic_phase1_direct -- realistic_phase1_direct/r3_rocksdb_cold_load
```

Export consolidated Criterion artifacts (JSON + markdown summary) from `target/criterion`:

```bash
pnpm bench:realistic:export-criterion -- \
  --out bench-out/native/criterion_realistic_phase1.json \
  --summary-md bench-out/native/criterion_realistic_phase1.md
```

The `criterion_realistic_phase1.*` artifact filenames are retained for history
ingestion compatibility. New Criterion output is filtered from the active
`realistic_phase1_direct/` prefix by default.

## Browser Runner (OPFS Worker)

Run the browser benchmark test:

```bash
pnpm --filter jazz-napi build
pnpm --dir packages/jazz-tools run bench:realistic:browser
```

The test runs against a real Chromium worker + OPFS runtime and emits JSON summaries to stdout.
The Node-side Jazz server used by the browser harness comes from `jazz-napi`, so its native binding needs to be built first in a workspace checkout.

The browser benchmark sets `logLevel: "warn"` in `DbConfig` so WASM tracing output stays quiet.

### Browser CPU profiling

For focused local CPU profiles of the browser runtime, run:

```bash
pnpm --dir crates/jazz-napi run build
pnpm bench:realistic:profile-browser -- --scenario w4,b2
```

This launches:

- a Vite dev server rooted at `packages/jazz-tools`
- a headless Chromium with CDP enabled
- a real `JazzServer` plus `TestJwtIssuer` from `jazz-napi`

It captures both main-thread and dedicated-worker CPU profiles to `/tmp/jazz-browser-profiles`
by default, then prints the hottest self-time frames for each scenario.

Useful options:

```bash
pnpm bench:realistic:profile-browser -- --scenario b3 --large-multiplier 4
pnpm bench:realistic:profile-browser -- --out-dir ./tmp/browser-profiles
```

Current browser scenarios:

- `W1`: interactive local workload mix (worker/OPFS)
- `W4`: cold reopen/query (worker/OPFS)
- `B1`: server-connected CRUD sustained throughput
- `B2`: server-connected read/load sustained throughput
- `B3`: server-connected cold load over larger dataset
- `B4`: fanout delivery latency/throughput across many subscribers
- `B5`: recursive policy schema read/update stress
- `B6`: hotspot deep-history update stress with storage usage sampling

## CI / Runner

- Workflow: `.github/workflows/benchmarks.yml`
- AWS setup: `dev/benchmarks/realistic/aws_runner_setup.md`

Artifacts include `manifest.json` as a stable ingestion entrypoint:

- native RocksDB: `bench-out/native/rocksdb/manifest.json`
- native SQLite: `bench-out/native/sqlite/manifest.json`
- jazz-sim: `bench-out/native/jazz-sim/manifest.json`
- browser: `bench-out/browser/manifest.json`

The workflow currently:

- runs on `main` pushes and nightly schedule
- runs on PRs only when the PR has the `benchmark` label
- runs every benchmark with a 60-second CI budget and records `passed`, `timed_out`, `failed`, or `skipped_configured`
- keeps a checked-in skip set at `dev/benchmarks/realistic/ci_skip_set.json`
- only activates configured skips after 3 timed-out observations for the same benchmark id
- records native example outputs (`W1`/`W4`) plus exported active direct Criterion results (`native-criterion`) when they complete within budget
- records jazz-sim JSONL outputs and logs under `bench-out/native/jazz-sim`; `update_history.mjs` ingests passed JSONL outputs into history as suite `jazz-sim`, keyed by scenario plus phase/variant, and exposes numeric JSONL fields to the static site/report as per-phase metrics
- records browser outputs per scenario when they complete within budget

The `site` job:

- pulls the benchmark artifacts for that run
- updates `history/bench_history.json` with absolute metrics
- updates `ci_skip_set.json` with newly observed timed-out benchmarks
- rebuilds `site/index.html` + `site/history.json`
- uploads `site/` as an artifact
- posts a markdown comparison report back to labeled PRs, including current-run status summaries
- commits refreshed history/site/skip-set back to `main` when the workflow itself runs on `main`

## Delta Rendering (Local)

After downloading artifacts for two runs (e.g. `main` vs branch), render deltas:

```bash
pnpm bench:realistic:render -- \
  --base ./artifacts/main \
  --head ./artifacts/branch \
  --kind all
```

Notes:

- Script: `dev/benchmarks/realistic/render_deltas.mjs`
- It auto-discovers `manifest.json` recursively under `--base` and `--head`.
- It compares the newest native/browser manifests found in each tree.
- For history-backed markdown reports in CI, use `dev/benchmarks/realistic/render_history_report.mjs`.

## Static Site (Local + Vercel)

Build locally from raw artifacts:

```bash
pnpm bench:realistic:update-history -- \
  --history dev/benchmarks/realistic/history/bench_history.json \
  --native ./site-input/native \
  --browser ./site-input/browser

pnpm bench:realistic:build-site -- \
  --history dev/benchmarks/realistic/history/bench_history.json \
  --out dev/benchmarks/realistic/site
```

For Vercel hosting:

- set project root directory to `dev/benchmarks/realistic/site`
- keep [vercel.json](site/vercel.json) in that directory so routing stays explicit
- use no install command and no build command (prebuilt static files)
- framework preset: `Other`
- deploy from `main` so each benchmark CI run refreshes the dashboard automatically
