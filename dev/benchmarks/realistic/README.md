# Realistic E2E Benchmarks

Shared benchmark definitions for the realistic, scenario-driven benchmark suite.
The program plan that assigns these workloads to the forthcoming interactive
example apps is [Examples & Benchmarks Program](../../EXAMPLES_AND_BENCHMARKS_PROGRAM.md).

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

These definitions are run locally only. CI performance coverage lives in the
hero examples' CodSpeed benches, which own these workloads (see the program
plan above).

## Native Runner

Run from workspace root:

```bash
RUST_LOG=warn cargo run --release -p jazz-native-transport --example realistic_bench -- \
  --profile dev/benchmarks/realistic/profiles/s.json \
  --scenario dev/benchmarks/realistic/scenarios/w1_interactive.json
```

`W3` requires a running server and `--server-url`:

```bash
RUST_LOG=warn cargo run --release -p jazz-native-transport --example realistic_bench -- \
  --profile dev/benchmarks/realistic/profiles/s.json \
  --scenario dev/benchmarks/realistic/scenarios/w3_offline_reconnect.json \
  --server-url http://127.0.0.1:1625
```

## Browser Runner (IndexedDB Worker)

Run the browser benchmark test:

```bash
pnpm --filter jazz-napi build
pnpm --dir packages/jazz-tools run bench:realistic:browser
```

The test runs against a real Chromium worker + IndexedDB runtime and emits JSON summaries to stdout.
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

- `W1`: interactive local workload mix (worker/IndexedDB)
- `W4`: cold reopen/query (worker/IndexedDB)
- `B1`: server-connected CRUD sustained throughput
- `B2`: server-connected read/load sustained throughput
- `B3`: server-connected cold load over larger dataset
- `B4`: fanout delivery latency/throughput across many subscribers
- `B5`: recursive policy schema read/update stress
- `B6`: hotspot deep-history update stress with storage usage sampling
- `B7`: large relation-result hydration over the project-board dataset

## History

The Realistic Benchmarks workflow and its self-hosted `jazz-bench` runner were
retired on 2026-10-15 (the runner had been offline since late September). Its
CI harness, checked-in timing history, static site and runner provisioning
scripts were removed with it; they remain in git history before the commit that
removed them.
