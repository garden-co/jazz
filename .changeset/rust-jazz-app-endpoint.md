---
"jazz-tools": patch
---

**Rust (`jazz-app-support`): the native `/jazz` endpoint is unstable, behind the off-by-default `unstable-endpoint` feature. It is not part of alpha.66 and will ship in a later release.**

The endpoint includes `Endpoint`, `serve_socket`, the `axum` router, `for_user` and `backend-link`. Its code stays in the tree and is tested in CI with the feature on, but the default build doesn't export it. Run an app's main backend with TS `createJazz`; from Rust, connect as a client.
