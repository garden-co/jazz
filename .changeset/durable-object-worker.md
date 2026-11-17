---
"jazz-tools": patch
---

New entry, `jazz-tools/durable-object` (workerd only): the Worker side of the `durableObjects()` store. Export `JazzCore` from your Worker and forward requests with `routeJazzRequest`; the bindings are `JAZZ` (a SQLite-backed Durable Object namespace of class `JazzCore`), `JAZZ_APP_ID` and `JAZZ_CORE_SECRET`. See the new "Cloudflare Durable Objects" deployment page. Experimental.
