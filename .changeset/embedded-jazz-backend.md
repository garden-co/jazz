---
"jazz-tools": patch
"jazz-napi": patch
---

Add `createJazz` to `jazz-tools/backend`: Jazz runs inside your backend's process with `sqlite({ path })` or `memory()`, serves sync and sign-in same-origin under `/jazz`, and applies the schema, migrations and permissions from code when it opens, with no separate server or deploy step. Mount it with `jazz.fetch`, `jazz.next()` (WebSocket on Vercel through `@vercel/functions`) or `jazz.nodeHandler` plus `jazz.attach` (`ws`). Browsers connect with `serverUrl: "/jazz"` and `session.loginWithBackend()`: the backend's `authenticate` names the user on sign-in and on every sync connection, so the browser holds no Jazz credential. Browsers fall back to an HTTP stream when the host can't upgrade sockets.
