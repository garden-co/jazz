# Deployment helper scripts

## Skip new-core Vercel previews

`skip-new-core-vercel-preview.mjs` is an intentionally fail-open Vercel
Ignored Build Step helper. It skips only pull-request previews whose GitHub
base branch is `codex/jazz-core-engine-swap`; all other cases continue their
build. Production deployments always continue.

`jazz2-inspector` is configured in its checked-in `vercel.json`. `jazz2-docs`
has no checked-in Vercel configuration, so the following dashboard setting is a
required provisioning prerequisite: merging this change alone does **not**
change docs builds.

Configure `jazz2-docs` under **Settings → Build and Deployment → Ignored Build
Step**:

For `jazz2-docs` (repository root):

```sh
node dev/scripts/skip-new-core-vercel-preview.mjs
```

Also add `VERCEL_GITHUB_READ_TOKEN` as an encrypted Vercel environment
variable for **Preview only**. It must be a fine-grained GitHub token restricted
to `garden-co/jazz` with **Pull requests: Read-only** permission.
Vercel must have **Automatically expose System Environment Variables** enabled
so `VERCEL_GIT_REPO_OWNER`, `VERCEL_GIT_REPO_SLUG`, and
`VERCEL_GIT_PULL_REQUEST_ID` are available. The helper logs neither its token
nor the GitHub response. Keep Vercel **Git Fork Protection** enabled and never
authorize an untrusted fork deployment while this token is configured.

The command exits `0` only after GitHub positively reports the exact new-core
base branch within five seconds; Vercel interprets `0` as skip and `1` as
continue building.
