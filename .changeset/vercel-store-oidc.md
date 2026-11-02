---
"jazz-tools": patch
"jazz-napi": patch
---

`vercel()` store: the function's Vercel OIDC token is the core's only credential. The core in the Sandbox admits only tokens naming the workload (team, project and environment) its first start pinned, keeps no secrets on its Drive (only the app id and that pin), and a deployment of another environment refuses to start or retarget it. Preview branches share one workload. `startLocalJazzServer` takes `trustedWorkload` for testing such a core.
