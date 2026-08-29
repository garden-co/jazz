# Deployment helper scripts

## Inspector Vercel preview filter

`skip-new-core-vercel-preview.mjs` is used only by the checked-in Inspector
Vercel configuration. It is an intentionally fail-open Ignored Build Step
helper: it skips pull-request previews targeting
`codex/jazz-core-engine-swap`; all other cases continue their build.

Its details are intentionally local to the Inspector project. Docs previews are
owned by `.github/workflows/docs-vercel-preview.yml`; no docs Vercel Ignore
Build Step is required.
