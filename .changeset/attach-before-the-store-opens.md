---
"jazz-tools": patch
---

`jazz.attach(server)` no longer opens the store, so a main backend whose `remote()` core is down at start keeps running instead of crashing on a bare "fetch failed". Until the core is reachable, Jazz routes and sync sockets answer 503 with an error that names the core's URL, and the next use tries again. `attach()` accepts every server Hono's `serve()` returns, and Jazz's sync sockets no longer reach the server's other upgrade listeners, which kept a custom Next.js server from syncing over WebSockets.
