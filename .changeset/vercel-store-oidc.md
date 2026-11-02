---
"jazz-tools": patch
"jazz-napi": patch
---

`vercel()` store: the function's Vercel OIDC token is the core's only credential. The core in the Sandbox trusts exactly the workload that token names (its project and environment) and keeps no secrets on its Drive, only the app id. `startLocalJazzServer` takes `trustedWorkload` for testing such a core.
