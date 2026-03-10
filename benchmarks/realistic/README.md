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

## Native Runner (Fjall)

Run from workspace root:

```bash
RUST_LOG=warn cargo run -p jazz-tools --features client --example realistic_bench -- \
  --profile benchmarks/realistic/profiles/s.json \
  --scenario benchmarks/realistic/scenarios/w1_interactive.json
```

`W3` requires a running server and `--server-url`:

```bash
RUST_LOG=warn cargo run -p jazz-tools --features client --example realistic_bench -- \
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
- scenario `R2B`: `benchmarks/realistic/scenarios/r2_reads_with_churn.json` (5% background write churn)
- scenario `R3`: `benchmarks/realistic/scenarios/r3_cold_load_fjall.json` (cold open + first query, Fjall)
- scenario `R4`: `benchmarks/realistic/scenarios/r4_fanout_updates.json` (N={10,50,200} subscribers)
- scenario `R5`: `benchmarks/realistic/scenarios/r5_permission_recursive.json` (recursive policy read/update with allow+deny mix)
- scenario `R6`: `benchmarks/realistic/scenarios/r6_permission_write_heavy.json` (recursive policy write-heavy allow+deny mix)
- scenario `R7A`: `benchmarks/realistic/scenarios/r7_hotspot_history.json` (deep updates on a small hot set)

Current topology coverage:

- `T0_local`: `realistic_phase1/crud_sustained` and `realistic_phase1/reads_sustained`
- mixed read/write churn: `realistic_phase1/reads_sustained_with_write_churn`
- `T1_single_hop`: `realistic_phase1/crud_sustained_single_hop` and `realistic_phase1/reads_sustained_single_hop`
- persisted cold-load (`M1_fjall`): `realistic_phase1/cold_load_fjall` (requires `--features fjall`)
- fanout delivery: `realistic_phase1/fanout_updates`
- recursive permission read/write: `realistic_phase1/permission_recursive`
- recursive permission write-heavy: `realistic_phase1/permission_write_heavy`
- hotspot deep-history updates: `realistic_phase1/hotspot_history`

Run only the cold-load benchmark:

```bash
cargo bench -p jazz-tools --features fjall --bench realistic_phase1 cold_load_fjall
```

Export consolidated Criterion artifacts (JSON + markdown summary) from `target/criterion`:

```bash
pnpm bench:realistic:export-criterion -- \
  --out bench-out/native/criterion_realistic_phase1.json \
  --summary-md bench-out/native/criterion_realistic_phase1.md
```

## Browser Runner (OPFS Worker)

Run the browser benchmark test:

```bash
pnpm --dir packages/jazz-tools run bench:realistic:browser
```

The test runs against a real Chromium worker + OPFS runtime and emits JSON summaries to stdout.

The browser benchmark sets `logLevel: "warn"` in `DbConfig` so WASM tracing output stays quiet.

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
- AWS setup: `benchmarks/realistic/aws_runner_setup.md`

Artifacts include `manifest.json` as a stable ingestion entrypoint:

- native: `bench-out/native/manifest.json`
- browser: `bench-out/browser/manifest.json`

The workflow currently:

- runs on `main` pushes and nightly schedule
- runs on PRs only when the PR has the `benchmark` label
- runs every benchmark with a 60-second CI budget and records `passed`, `timed_out`, `failed`, or `skipped_configured`
- keeps a checked-in skip set at `benchmarks/realistic/ci_skip_set.json`
- records native example outputs (`W1`/`W4`) plus exported Criterion results (`native-criterion`) when they complete within budget
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

- Script: `benchmarks/realistic/render_deltas.mjs`
- It auto-discovers `manifest.json` recursively under `--base` and `--head`.
- It compares the newest native/browser manifests found in each tree.
- For history-backed markdown reports in CI, use `benchmarks/realistic/render_history_report.mjs`.

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
- keep [vercel.json](/Users/anselm/.codex/worktrees/30d1/jazz2/benchmarks/realistic/site/vercel.json) in that directory so routing stays explicit
- use no install command and no build command (prebuilt static files)
- framework preset: `Other`
- deploy from `main` so each benchmark CI run refreshes the dashboard automatically
