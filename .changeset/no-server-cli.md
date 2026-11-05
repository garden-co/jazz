---
"jazz-tools": patch
---

The `jazz-tools server` and `jazz-tools create` commands are removed, and the package no longer ships native binaries. Jazz runs inside your backend: `createJazz({ store: sqlite({ path }) })` serves sync from your own server and `createJazz({ store: remote({ url, appId, secret }) })` uses Jazz Cloud. Start a project with `npm create jazz`. The CLI keeps `validate`, `schema compile`, `migrations create` and `migrations graph`, and the removed commands say what replaced them.
