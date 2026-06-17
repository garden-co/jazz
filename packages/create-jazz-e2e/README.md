# create-jazz-e2e

End-to-end harness that builds each `create-jazz` starter the way a real user
would receive it, then runs the starter's Playwright suite against the
production build.

This catches a class of bugs that the per-starter Playwright suites miss —
they run against `pnpm dev`, where the Vite/Turbopack pipelines mask build-time
problems (e.g. duplicate React copies under Next's SSR worker, jazz-tools/jazz-napi
resolution differences between dev and prod).

## How it works

For each starter, the harness:

1. **Packs** the workspace packages a starter can transitively pull in
   (`jazz-tools`, `jazz-napi`, `jazz-wasm`) into `.tgz` tarballs.
2. **Scaffolds** via the `create-jazz` CLI itself, with `JAZZ_STARTER_PATH`
   pointing at the local starter dir and `--hosting selfhosted` so it never
   reaches Jazz Cloud.
3. **Pins** the tarballs into the scaffolded `package.json` via
   `pnpm.overrides`. This lets `pnpm install` work even when the alpha versions
   the workspace bumps to aren't yet on npm (the normal case during a release PR).
4. **Installs** with `pnpm install --ignore-workspace`.
5. **Starts a local Jazz sync server** in-process (via `jazz-tools/dev`'s
   `startLocalJazzServer`) and writes its app ID + URL + backend secret into
   the scaffolded `.env`. No traffic leaves the runner.
6. **Builds** the starter (`pnpm build`).
7. **Runs Playwright** with `JAZZ_E2E_PROD=1` set. Each starter's
   `playwright.config.ts` reads that flag and swaps its `webServer.command`
   from `pnpm dev` to the framework-appropriate production start
   (`next start`, `vite preview`, `node build`, `node server-dist/index.js`).

## Running locally

```bash
# One starter.
pnpm build:core
pnpm --filter create-jazz-e2e exec tsx src/cli.ts next-localfirst

# All twelve, sequentially.
pnpm --filter create-jazz-e2e exec tsx src/cli.ts --all

# Stream all child output (verbose).
pnpm --filter create-jazz-e2e exec tsx src/cli.ts next-localfirst --verbose

# Skip the playwright step (build-only smoke).
pnpm --filter create-jazz-e2e exec tsx src/cli.ts --all --skip-e2e

# Keep the scaffolded tempdir around after a failure for inspection.
pnpm --filter create-jazz-e2e exec tsx src/cli.ts next-localfirst --keep

# Skip the in-process `pnpm pack` step and consume prebuilt tarballs
# (one *.tgz per package in PACKAGES_TO_PACK). Used by CI's prepare job.
pnpm --filter create-jazz-e2e exec tsx src/cli.ts next-localfirst --tarball-dir ./_e2e-state/tarballs
```

Browsers must be installed once per machine:

```bash
pnpm exec playwright install chromium --with-deps
```

## CI

`.github/workflows/starters-e2e.yml` is split into two jobs to avoid repeating
the ~10-minute `build:core` across 12 matrix runners:

1. `prepare` — builds the workspace once, packs the three workspace tarballs,
   and uploads them alongside the runtime artifacts the harness needs
   (`jazz-tools/dist`, jazz-napi's `index.js` + native `.node`) as a single
   workflow artifact.
2. `e2e` (matrix) — downloads that artifact, installs Playwright browsers, and
   runs the harness with `--tarball-dir` so it skips the pack step.

Fires on every push to a `changeset-release/*` branch (the release PR that the
changesets action keeps updated against `main`). Failures block the release.
`workflow_dispatch` is also available for ad-hoc runs and accepts a single
`starter` input to run just one matrix slot.
