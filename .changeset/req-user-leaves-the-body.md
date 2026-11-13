---
"jazz-tools": patch
---

`JazzBackend.forCurrentReqUser()` (`jazz-tools/effect/backend`) no longer takes the request body from the handler it wraps. It handed `authenticate` the whole request, body stream included, and on some effect versions (4.0.0-rc.118) that consumed the body, so a handler's `HttpServerRequest.schemaBodyJson` then failed. `authenticate` now gets the request's method, URL and headers, which is all it needs to decide who is signed in; the body stays for the handler.
