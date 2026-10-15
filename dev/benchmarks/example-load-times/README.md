# Example app load times

These cases time what a user waits for in ten example apps: signing up, reloading, opening an
invite, and seeing another device's edit. Each case drives the app's **production build** in
Chromium through Playwright. The Vitest benchmark runner with the CodSpeed plugin measures it.

Each case is named `<app>_<scenario>_browser`, for example `band_book_reload_browser`. It times
one user action until the named content is visible. `metadata.ts` lists every case with its start
action, the content it waits for, and the state it starts from. The docs examples page shows the
same scenarios.

## What runs

- **Builds.** `build.mjs` runs `next build` for the Next.js apps. For epic-drop, stage-plan and
  world-tour it runs `vite build`. Each build gets the env its servers use (`apps.mjs`): the app's
  origin, its app ID, its sync server URL and throwaway secrets. jamazon also gets
  `PAYMENT_PROVIDER=sandbox`.
- **Servers.** Each app runs three local processes:
  - `next start` or `vite preview`;
  - a CDN-like front (`cdn.mjs`) on the app's port. It serves the hashed assets as a CDN would:
    the real content type (`application/wasm` for the runtime), precompressed brotli or gzip, and
    `immutable` caching. It proxies everything else to the app server;
  - a local JazzServer (`sync.mjs`) with a fresh data dir. It uses the app's own server options
    and deploys the app's schema and permissions.
- **Fixtures.** Before an app's cases run, `setup` creates its fixture users and content. This is
  untimed: for example, an owner signed up and on the measured page. The cases then run in
  `metadata.ts` order.
- **Iterations.** Each iteration runs the scenario's `prepare` first, untimed: a fresh browser
  context on a filled form, a newly signed-up member with an invite link, and so on. Then it times
  `run`. Three rounds per case run by default.
- **Sign-up rate limit.** Better Auth rate-limits sign-ups and sign-ins in production builds.
  `rateLimitGate` waits out its window, untimed, before a request would be refused.

App builds, asset precompression, server startup, browser launch, fixture sign-ups and every setup
step stay outside the timed region. Chromium runs headless with its default rendering settings. It
does not use the SwiftShader GL flags: none of the apps needs WebGL, and with those flags Chromium
spent most of the runner's cores on rasterising World Tour's globe, which slowed every page beside
it. Everything runs on localhost. There is no latency-throttled variant.

## Wall time, not instruction counts

These cases run in CodSpeed's **walltime** mode on the `codspeed-macro` runner. Instruction-count
simulation cannot run a browser. The numbers are low-noise, because the runner is dedicated, but
they are not deterministic: scheduling, the browser and the sync round trips all vary between
runs. Compare trends and CodSpeed's regression warnings, not single runs.

## Run

The apps need the release WASM/NAPI artifacts and the compiled Jazz Tools runtime. Then:

```sh
node dev/artifacts/verify-starter-e2e-artifacts.mjs
pnpm --filter jazz-example-load-times-benchmark build:apps   # all ten apps, a few minutes
pnpm --filter jazz-example-load-times-benchmark exec playwright install chromium
pnpm --filter jazz-example-load-times-benchmark bench
```

For a quick check:

```sh
cd dev/benchmarks/example-load-times
node build.mjs stage-plan
BENCH_ROUNDS=1 BENCH_FILTER=stage_plan pnpm bench benches/stage-plan.bench.mjs
```

- `BENCH_ROUNDS` sets the iterations per case.
- `BENCH_FILTER` selects cases by substring.
- `CHROMIUM_PATH` points at an installed Chromium instead of Playwright's.
- `BENCH_TRACE=1` logs each iteration's untimed `prepare` and `finish` times, its timed `run`, the
  CPU time each process group used during the run, and what the pages logged, loaded and sent over
  WebSocket meanwhile. Reading the CPU times adds a little to each `run`, so it stays off on CI.

Each app has its own bench file. A failing case drops only its app's results.
The runner writes results to `.generated/results.json`, and keeps pidfiles for the servers'
process groups in `.generated/pids`. The next run kills anything a crashed run left behind.

## CI

The cases belong to the **nightly suite** of `.github/workflows/codspeed.yml`. They run on the
scheduled nightly run on main, and on a manual dispatch with `suite: nightly`. They never run on
pull requests or on main merges, so they add nothing to a PR's checks or to a merge's CodSpeed
report. They take about 17 min on `codspeed-macro`.

The nightly suite runs three jobs for them:

1. **Text editor build** (Blacksmith). It builds the release runtime. The merge suite runs it too,
   for the text editor's own browser cases.
2. **Example apps build** (Blacksmith). It builds the ten apps against that runtime and
   precompresses their assets. Both runtime and builds are uploaded as artifacts.
3. **Measurement** (`codspeed-macro`). It installs dependencies, unpacks both artifacts, verifies
   the runtime's fingerprints and runs these cases. Nothing is compiled there.

### Measuring a pull request on demand

CodSpeed compares a branch's results with main's newest results for the same cases, so measure
both at the same point:

1. Bring the branch up to date with main (merge main into it).
2. Dispatch the workflow with `suite: nightly` on main, unless tonight's scheduled run already
   measured main's current commit.
3. Dispatch it with `suite: nightly` on the branch.

```sh
gh workflow run codspeed.yml --ref main -f suite=nightly
gh workflow run codspeed.yml --ref <branch> -f suite=nightly
```

The dispatch on the branch also runs the native workloads' nightly extras. Its CodSpeed report
compares the load-time cases with main's run.

Descriptions shown on the docs page come from `metadata.ts` through
[../metadata](../metadata/README.md).
