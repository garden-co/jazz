---
"jazz-tools": patch
---

Add `createJazzBackend()` to `jazz-tools/backend`. It admits the process with `backendSecret` and resolves to a ready backend client (`db`, `forRequest`, `forAccount`, `withAttribution*`, `close()`), or rejects when admission or opening fails. Backends no longer read `session.getSnapshot()` and check `status` before using the client. `createJazzSession` stays for code that switches accounts.
