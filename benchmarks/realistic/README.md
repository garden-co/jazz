# Realistic E2E Benchmarks

Shared benchmark definitions for the realistic, scenario-driven benchmark suite.

## Files

- `schema/project_board.schema.json`: canonical collaborative app schema
- `profiles/s.json`: smoke/team-dev profile (`S`)
- `profiles/m.json`: medium profile (`M`)
- `scenarios/w1_interactive.json`: read-heavy interactive session
- `scenarios/w3_offline_reconnect.json`: offline writes then reconnect
- `scenarios/w4_cold_start.json`: reopen and first-query latency

## Native Runner (SurrealKV)

Run from workspace root:

```bash
RUST_LOG=warn cargo run -p jazz-rs --example realistic_bench -- \
  --profile benchmarks/realistic/profiles/s.json \
  --scenario benchmarks/realistic/scenarios/w1_interactive.json
```

`W3` requires a running server and `--server-url`:

```bash
RUST_LOG=warn cargo run -p jazz-rs --example realistic_bench -- \
  --profile benchmarks/realistic/profiles/s.json \
  --scenario benchmarks/realistic/scenarios/w3_offline_reconnect.json \
  --server-url http://127.0.0.1:1625
```

## Rust Criterion (Phase 1 local baseline)

Run the local realistic benchmark suite:

```bash
cargo bench -p jazz-tools --bench realistic_phase1
```

It currently loads:

- profile: `benchmarks/realistic/profiles/s.json`
- scenario `R1`: `benchmarks/realistic/scenarios/r1_crud_sustained.json`
- scenario `R2`: `benchmarks/realistic/scenarios/r2_reads_sustained.json`

Current topology coverage:

- `T0_local`: `realistic_phase1/crud_sustained` and `realistic_phase1/reads_sustained`
- `T1_single_hop`: `realistic_phase1/crud_sustained_single_hop`

## Browser Runner (OPFS Worker)

Run the browser benchmark test:

```bash
pnpm --dir packages/jazz-tools run bench:realistic:browser
```

The test runs against a real Chromium worker + OPFS runtime and emits JSON summaries to stdout.

The browser benchmark sets `logLevel: "warn"` in `DbConfig` so WASM tracing output stays quiet.

## CI / Runner

- Workflow: `/Users/anselm/.codex/worktrees/f472/jazz2-clean/.github/workflows/benchmarks.yml`
- AWS setup: `/Users/anselm/.codex/worktrees/f472/jazz2-clean/benchmarks/realistic/aws_runner_setup.md`

Artifacts include `manifest.json` as a stable ingestion entrypoint:

- native: `bench-out/native/manifest.json`
- browser: `bench-out/browser/manifest.json`

The workflow also has a `site` job that:

- pulls the benchmark artifacts for that run
- updates `history/bench_history.json` with absolute metrics
- rebuilds `site/index.html` + `site/history.json`
- uploads `site/` as an artifact
- commits refreshed history/site back to `main` when the workflow itself runs on `main`

## Delta Rendering (Local)

After downloading artifacts for two runs (e.g. `main` vs branch), render deltas:

```bash
pnpm bench:realistic:render -- \
  --base ./artifacts/main \
  --head ./artifacts/branch \
  --kind all
```

Notes:

- Script: `benchmarks/realistic/render_deltas.mjs`
- It auto-discovers `manifest.json` recursively under `--base` and `--head`.
- It compares the newest native/browser manifests found in each tree.

## Static Site (Local + Vercel)

Build locally from raw artifacts:

```bash
pnpm bench:realistic:update-history -- \
  --history benchmarks/realistic/history/bench_history.json \
  --native ./site-input/native \
  --browser ./site-input/browser

pnpm bench:realistic:build-site -- \
  --history benchmarks/realistic/history/bench_history.json \
  --out benchmarks/realistic/site
```

For Vercel hosting:

- set project root directory to `benchmarks/realistic/site`
- use no install command and no build command (prebuilt static files)
- deploy from `main` so each benchmark CI run refreshes the dashboard automatically
