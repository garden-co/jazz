---
"jazz-tools": patch
"jazz-napi": patch
---

Add `createJazz` to `jazz-tools/backend`: Jazz runs inside your backend's process with `sqlite({ path })` or `memory()` (or in front of a sync server elsewhere, such as Jazz Cloud, with `remote({ url, adminSecret, backendSecret })`), serves sync and sign-in same-origin under `/jazz`, and applies the schema, migrations and permissions from code when it opens, with no separate server or deploy step. Mount it with `jazz.fetch`, `jazz.next()` (WebSocket on Vercel through `@vercel/functions`) or `jazz.nodeHandler` plus `jazz.attach` (`ws`). Browsers connect with `serverUrl: "/jazz"` and `session.loginWithBackend()`: the backend's `authenticate` names the user on sign-in and on every sync connection, so the browser holds no Jazz credential, and `jazz.linkIdentity` links a guest's or second sign-in method's identity to an existing account. Browsers fall back to an HTTP stream when the host can't upgrade sockets.
