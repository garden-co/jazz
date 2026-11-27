# App comparisons: the Jazz hero apps on other sync stacks

A first signal on **relative performance** and **lines of code** of Jazz against other sync stacks. It uses
four of our hero example apps, each ported to five other stacks with the same UI, the same fixtures and
the same permission rules, enforced on the server. It is a first signal, not a benchmark suite: every
number says what it measured and where. The fairness contract (porting rules, scenarios, metrics) is in
[`SPEC.md`](SPEC.md).

The findings are in the reports. Their numbers are unchanged from the runs that produced them:
- [`results/2026-11-14/REPORT.md`](results/2026-11-14/REPORT.md): v3, every hero on every stack (Jazz on
  alpha.62), plus the profiling write-ups next to it (`thin-startup/`, `lf-startup/`, `lf-delivery/`,
  `breakdown/`, `bundle-size/`, `biglabel-writes/`, `LOW-HANGING-FRUIT.md`);
- [`results/2026-11-15/REPORT.md`](results/2026-11-15/REPORT.md): v4, what alpha.64 changed for Jazz;
- [`results/2026-11-17/REPORT.md`](results/2026-11-17/REPORT.md): the alpha.66 regression check.

This code was developed in a separate repository and moved here. Its
[raw captures (team-only)](https://github.com/garden-co/jazz-app-comparisons), its history and the port
PRs stay there: CPU profiles, traces, debug logs and run logs, per-sample JSON over 64 KB, bundle
treemaps. The reports' `#name` references point to internal team threads.

## Where everything is

| what | where |
|---|---|
| harness, scenarios, adapters | `dev/app-comparisons/harness/` |
| fixtures (stack-agnostic, deterministic) | `dev/app-comparisons/fixtures/` |
| spec, reports, result JSON | `dev/app-comparisons/SPEC.md`, `dev/app-comparisons/results/<date>/` |
| the ports: `<hero>/<stack>` | next to the hero's Jazz app, named after the Jazz variant plus the stack (below) |
| the Jazz side: `<hero>/jazz` | the real upstream hero app, with its bench hooks in a `comparison/` folder |

| hero | Jazz app (`<hero>/jazz`) | ports (`<hero>/<stack>`) |
|---|---|---|
| StagePlan | `examples/stage-plan/apps/react-localfirst` | `examples/stage-plan/apps/react-<stack>` |
| BandChat | `examples/band-chat/apps/react-hono-betterauth` | `examples/band-chat/apps/react-hono-<stack>` |
| Wequencer | `examples/wequencer/apps/next-betterauth` | `examples/wequencer/apps/next-<stack>` |
| BigLabel | `examples/big-label` | `examples/big-label/apps/next-<stack>` |

Stacks: `zero` (Rocicorp Zero, local zero-cache + Postgres), `convex` (local anonymous backend),
`instantdb` (hosted temporary app), `postgres` (plain Postgres + hand-written sync), `neon` (Neon
Postgres + Realtime, hosted). `harness/apps.mjs` maps a target to its directory.

Each port is a standalone pnpm project: its own `package.json`, `pnpm-lock.yaml` and
`pnpm-workspace.yaml`, plus `harness.mjs` (the hooks the harness calls), `loc.json` (what counts as
schema, permissions, server, data and UI) and `NOTES.md` (versions, approximations, what didn't work).
The harness's hook contract is in [`harness/README.md`](harness/README.md).

## Running it

```sh
cd dev/app-comparisons/harness && pnpm install --ignore-workspace
node run.mjs stage-plan/zero --runs 1 --samples 2        # one target: seed, build, serve, measure
node run.mjs stage-plan/jazz --variant thin              # Jazz variants: default, now, thin
node loc.mjs                                             # lines of code, every target
node report.mjs results/<date>                           # markdown tables for a results directory
```

`run.mjs` writes `results/<date>/<hero>.<stack>.<variant>.json`. Each port installs itself on first use
(`pnpm install --frozen-lockfile` in its own directory). The Jazz targets use the workspace's
`jazz-tools`, so build it first (`pnpm build:core` at the repo root). Machine setup:
- Zero and plain Postgres need a local Postgres 18 with `wal_level = logical` (see
  `examples/stage-plan/apps/react-zero/NOTES.md`);
- Neon needs `NEON_API_KEY` or `neon login`;
- InstantDB creates a temporary app on Instant Cloud (network access).

## Kept out of every gate, for now

The ports aren't Jazz apps and aren't maintained with the repo, so nothing in CI builds, lints, formats,
typechecks or tests them, and they add no CI work. Each mechanism is one list, marked with a comment
pointing here:

- **pnpm workspace** (`pnpm-workspace.yaml`): a literal `!examples/<hero>/apps/<base>-<stack>` per
  port, so none is a workspace member. That keeps them out of turbo, `pnpm -r` (the examples
  typecheck), changesets and the root lockfile. `dev/app-comparisons/harness` matches no workspace
  glob either.
- **Lint and format** (`vite.config.ts`, `sharedIgnores`, used by `vp lint`, `vp fmt` and the
  pre-commit hook): `examples/*/apps/*-{zero,convex,instantdb,postgres,neon}/**`,
  `examples/**/comparison/**`, `dev/app-comparisons/**`.
- **Typecheck of the Jazz heroes**: `comparison/` is excluded in the BandChat and Wequencer
  `tsconfig.json`s (they include `**/*.ts`). The StagePlan and BigLabel tsconfigs only include `src/`.
- **examples-e2e** (`dev/examples-e2e/run.mjs`): its variant list is explicit, so no port is a variant.
  When it copies an app it skips `comparison/` and big-label's top-level `apps/`.
- **example-client-permissions** (`dev/gates/test/example-client-permissions.test.mjs`): `comparison/`
  counts as bench code, like `benchmarks/`, since StagePlan's `comparison/seed.ts` imports
  `jazz.permissions.json` for a backend `createJazz`.
- **No change needed**, because these use explicit lists or key on Jazz files the ports don't have
  (`jazz.app.json`, a `jazz-tools` dependency): CodSpeed and the bench discovery, example-load-times,
  example-bindings, example-app-files, example-ws-peer, the docs showcase catalogue and side nav,
  docs-snippets.
- **Local only**: `dev/gates/ignored-tests.mjs` walks `examples/` without skipping `node_modules`. After
  a port's `pnpm install`, a dependency's skipped test can trip it locally. Remove the port's
  `node_modules` before running that gate.

To bring a port into the gates later, delete its line from each list above.

## The Jazz side: upstream apps, bench hooks

The Jazz targets run the real hero apps, not copies. Each app gets:
- `comparison/harness.mjs` and `comparison/seed.ts`: seeding through the app's own server code, a
  production build and serve;
- `comparison/loc.json`;
- bench-only env switches in the app code, inert when unset (the app behaves as shipped).

| app | switch | variants |
|---|---|---|
| StagePlan, BandChat | `VITE_JAZZ_THIN=1` (thin client only), `VITE_JAZZ_LOCAL_FIRST=now` | `thin`, `now` |
| Wequencer | `NEXT_PUBLIC_JAZZ_THIN=1`, `NEXT_PUBLIC_JAZZ_LOCAL_FIRST=now` | `thin`, `now` |
| BigLabel | `NEXT_PUBLIC_JAZZ_LOCAL_FIRST=background` (thin first, local-first in the background, as the other heroes ship) or `=now` | `default`, `now` |
| StagePlan | `JAZZ_DATA_DIR` (the Vite backend's data directory) | bench state |

BigLabel ships thin-only upstream, so its harness variant `thin` is BigLabel as shipped, and
`default` sets `background`. The reports label it the same way.

**Drift between the comparison's Jazz copies (which the reports measured) and upstream:**
- **Versions.** The copies ran on published `jazz-tools` (2.0.0-alpha.62, then .64 and .66). Upstream
  runs the workspace's current `jazz-tools`. New runs are therefore not directly comparable with the
  reports.
- **Session API.** The copies use alpha.66's `useSession()?.user`; upstream uses
  `useJazzSession().user?.accountId`.
- **Permissions.** The copies' policies are what `jazz-tools upgrade` wrote for alpha.66 (helpers like
  `rule1`, `isCreatedByAccount`). Upstream's are hand-written with named helpers and comments. They are
  meant to be equivalent, but that hasn't been re-verified rule by rule.
- **BandChat.** The copy declares a `(roomId, $createdAt)` composite index for a room's newest page;
  upstream doesn't, and it's left out here. Adding it upstream is a follow-up that waits on
  [jazz#5632](https://github.com/garden-co/jazz/pull/5632). Upstream also gained a Cloudflare Worker
  deployment (`server/worker.ts`, `wrangler.jsonc`), which the harness doesn't use.
- **BigLabel.** The copy put the local-first client in by default; upstream gets it only through the
  env switch above. Upstream's README and e2e test have moved on; the measured UI paths are the same.
- **StagePlan, Wequencer.** Comments and README wording only; the schema hashes match.

## Left out

- The Jazz copies themselves (`<hero>/jazz` in the old repository): replaced by the upstream apps.
- The profiles' app instrumentation (`VITE_BENCH_MARKS` marks and the `jazz-tools` dist patches in
  the profile branches). `harness/breakdown.mjs`, `lf-delivery.mjs` and `biglabel-writes/` now point at
  the upstream apps, but without that instrumentation they record no app marks.
  `harness/lf-patches/` and `biglabel-writes/apply.py` patched a published dist and don't apply to the
  workspace build.
- Raw captures and run logs (see above).
