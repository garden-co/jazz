---
"jazz-tools": patch
"jazz-napi": patch
---

Add `createJazz` to `jazz-tools/backend`: Jazz runs inside your backend's process with `sqlite({ path })` or `memory()`, serves sync and sign-in same-origin under `/jazz`, and applies the schema, migrations and permissions from code when it opens, with no separate server or deploy step. Mount it with `jazz.fetch`, `jazz.next()` (WebSocket on Vercel through `@vercel/functions`) or `jazz.nodeHandler` plus `jazz.attach` (`ws`). Browsers connect with `serverUrl: "/jazz"` and `backendToken()`, and fall back to an HTTP stream when the host can't upgrade sockets.
