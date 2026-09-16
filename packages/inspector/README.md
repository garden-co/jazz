# Jazz Inspector

Inspector UI for exploring Jazz databases, usable either as a standalone web app or as an embedded development overlay.

## Running the Inspector in standalone mode

You can run the Inspector as a regular web app that connects to a Jazz sync server.

```sh
cd packages/inspector
pnpm dev
```

Then open `http://localhost:5173` in your browser (Vite’s default dev server port).

- **First-time configuration**
  - **serverUrl**: base URL of your Jazz cloud server (for example `https://api.my-jazz-cloud.com`).
  - **appId**: the Jazz app identifier you want to inspect.
  - **adminSecret**: admin secret for that app.
  - **env**: environment name (for example `dev`, `staging`, `prod`). Defaults to `dev` if left empty.
  - **branch**: logical branch name (defaults to `main`).

The inspector derives app-scoped endpoints automatically from `serverUrl` and `appId`, so there is
no separate path-prefix setting.

Saved connections retain connection details, but never the admin secret. Re-enter the secret after
reloading or opening a saved connection. Development links prefill non-secret details only; legacy
secrets in URL fragments and saved connection data are removed.

For a managed development server, copy the admin secret from the interactive terminal banner. If
your server runs without a TTY, configure a known secret using the integration's
`adminSecret` option before starting it, then enter that same secret in the Inspector.

## Building the Inspector

The package provides standalone web and embedded builds.

- **Build the standalone web app**

```sh
cd packages/inspector
pnpm build
```

- **Build the embedded inspector**

```sh
cd packages/inspector
pnpm build:embedded
```

The Jazz Vite and SvelteKit development integrations serve the embedded inspector as an in-app
overlay by default. Set their `inspector` option to `false` to disable it.

## Staging a release on Vercel

The package-build workflow builds the web app against its already verified Jazz
Tools/WASM artifacts, then uploads `inspector-prebuilt`. Its receipt binds every
output file (including Vercel routing) to the actual checked-out Git HEAD, not
`GITHUB_SHA` (which can identify a synthetic PR merge in preview workflows). It avoids another
native compilation on Vercel.

Run **Stage Inspector production** with the successful package-build run ID,
deployment SHA, and branch. The workflow checks the artifact run's repository and
success, proves its source tree equals the deployment SHA's tree, and checks the
deployment branch still points to that SHA using an explicit `refs/heads` lookup
(tags and raw commit aliases cannot substitute for the requested branch). Thus a release merge can reuse a
verified preview artifact only when the entire source tree (including package
versions) is identical. It verifies every downloaded output hash, then deploys with `--prebuilt
--prod --skip-domain`. This does not assign production domains. Run **Promote
inspector production** with that same SHA and branch after staging acceptance;
it resolves and promotes those exact bytes without rebuilding. Do not use a
preview-to-production source redeploy, which can change install/build settings.
The prebuilt artifact requires no Git checkout or Vercel-side install step.

Only these successful, same-repository producer callers are admitted:

- `preview-build.yml` on `pull_request` (its explicit checkout is the PR head).
- `preview-jazz-tools-alpha-release.yml` on `push` or `workflow_dispatch` from
  `changeset-release/*`.
- `publish-jazz-tools-alpha.yml` on `push` or `workflow_dispatch` from `release`.

Arbitrary workflows and manual publisher dry-runs on other branches are excluded
from production staging even if they upload an artifact with the same name.
The receipt SHA must match the admitted run's head SHA. Release merge reuse still
requires equality of the entire source tree.

For a local recovery from the downloaded artifact, first run
`node dev/scripts/inspector-prebuilt.mjs verify DIRECTORY SOURCE_SHA`. Only use an
artifact from the trusted successful workflow run for that SHA; the receipt is an
integrity check, not a signature. Keep the output intact and use the same prebuilt
staging flags and `githubCommitSha` / `githubCommitRef` metadata as the workflow.
Preserve the artifact source SHA separately in `releaseSourceSha`. The Vercel
project root is `packages/inspector`: copy the verified `.vercel/output` to
`packages/inspector/.vercel/output` as well before invoking the CLI from the
staging root. Both output locations are required by the CLI/project-root checks.

The three `VERCEL_INSPECTOR_*` GitHub secrets must identify one project and team
and a token with access to both deployment lookup and deployment/promotion. A 403
requires repairing this configuration; local CLI access does not establish that
the GitHub token works. Never remove the exact-SHA promotion check to recover.
