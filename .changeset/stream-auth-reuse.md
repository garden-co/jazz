---
"jazz-tools": patch
---

HTTP stream sessions no longer run the app's `authenticate` on every send. A send whose headers match the last one `authenticate` admitted reuses that answer, for at most 60 seconds and never past its `expiresAt`. Any changed header except per-request proxy, tracing and body headers asks again; `Cookie` and `Authorization` always count. `revokeUser` still ends a session at once. A sign-out or ban that skips `revokeUser`, or that reaches a stream on another instance, now cuts off the stream within a minute rather than on its next send; WebSocket sessions hold until `expiresAt` as before. This cuts a thin BigLabel save, five sends in a row, from five `getSession` lookups to one.
