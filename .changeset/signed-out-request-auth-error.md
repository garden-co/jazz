---
"jazz-tools": patch
---

`jazz.forReqUser(request)` and `jazz.withAttribution(request)` on a request that `authenticate` names no user for now throw a `RequestAuthenticationError` with `code: "signed_out"`, so `isRequestAuthenticationError(error)` maps it to a 401. They threw a plain `Error` before, which the documented mapping turned into a 500. An `authenticate` that throws still passes its own error through. In `jazz-tools/effect/backend`, the `JazzError`'s `cause` is that error, so a handler can tell a signed-out request from `authenticate` failing.
