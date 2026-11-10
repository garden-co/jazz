---
"jazz-tools": patch
---

`jazz.forRequest(request)` is now `jazz.forReqUser(request)`: it hands back a `Db` that reads and writes as the request's user. In `jazz-tools/effect/backend`, `JazzBackend.forRequest` and `JazzBackend.forCurrentRequest` are now `JazzBackend.forReqUser` and `JazzBackend.forCurrentReqUser`. The old names are gone, with no alias.
