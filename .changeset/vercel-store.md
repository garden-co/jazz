---
"jazz-tools": patch
---

Add the `vercel()` store for `createJazz`: the app's core runs in a persistent Vercel Sandbox in your own Vercel project, created on first use, woken on demand and stopped when idle. It authenticates with the function's OIDC token, so it needs no Jazz env vars, and runs a local SQLite core outside Vercel deployments.
