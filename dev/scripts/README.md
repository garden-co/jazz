# Deployment helper scripts

## Inspector Vercel preview filter

`skip-new-core-vercel-preview.mjs` is used only by the checked-in Inspector
Vercel configuration. It is an intentionally fail-open Ignored Build Step
helper: it skips pull-request previews targeting
`codex/jazz-core-engine-swap`; all other cases continue their build.

Its details are intentionally local to the Inspector project.

## Docs Vercel deployment filter

`skip-docs-vercel-deploy.mjs` is the fail-closed Ignored Build Step for the
`jazz2-docs` project. It permits only open, trusted, same-repository pull
requests carrying the `docs` label. All production events—including `main`—and
all failed or incomplete GitHub lookups are skipped.
