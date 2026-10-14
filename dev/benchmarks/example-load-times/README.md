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

App builds, asset precompression, server startup, browser launch, fixture sign-ups and every
setup step stay outside the timed region. Everything runs on localhost. There is no
latency-throttled variant.

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

Each app has its own bench file. A failing case drops only its app's results.
The runner writes results to `.generated/results.json`, and keeps pidfiles for the servers'
process groups in `.generated/pids`. The next run kills anything a crashed run left behind.

## CI

The CodSpeed workflow has three jobs:

1. **Text editor build** (Blacksmith, already present). It builds the release runtime.
2. **Example apps build** (Blacksmith). It builds the ten apps against that runtime and
   precompresses their assets. Both runtime and builds are uploaded as artifacts.
3. **Measurement** (`codspeed-macro`). It installs dependencies, unpacks both artifacts, verifies
   the runtime's fingerprints and runs these cases. Nothing is compiled there.

The cases belong to the merge suite. They run on every main merge and on pull requests with the
`benchmark` label.

Descriptions shown on the docs page come from `metadata.ts` through
[../metadata](../metadata/README.md).
