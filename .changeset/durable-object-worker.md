---
"jazz-tools": patch
---

New entry, `jazz-tools/durable-object` (workerd only): the Worker side of the `durableObjects()` store. Export `JazzCore` from your Worker and forward requests with `routeJazzRequest`; the bindings are `JAZZ` (a SQLite-backed Durable Object namespace of class `JazzCore`) and `JAZZ_CORE_SECRET`. The Worker's one object is the app: it keeps the app's data and its id. See the new "Cloudflare Durable Objects" deployment page. Experimental.
