---
"jazz-tools": patch
---

HTTP stream sessions no longer run the app's `authenticate` on every send. A send whose headers match the last one `authenticate` admitted reuses that answer, for at most `streamAuthReuseSeconds` (a new `createJazz` option, default 60) and never past its `expiresAt`. Only headers that proxies, hosts and tracing set per request are left out of that match; `Cookie`, `Authorization` and identity-proxy headers always count. `revokeUser` still ends a session at once. A sign-out or ban that skips `revokeUser`, a revoked scoped backend credential, or a revocation that reaches a stream on another instance now cuts off the stream within that window, rather than on its next send. Pass `streamAuthReuseSeconds: 0` if your `authenticate` must run on every request. WebSocket sessions hold until `expiresAt`, as before. This cuts a thin BigLabel save, five sends in a row, from five `getSession` lookups to one.
