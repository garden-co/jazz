---
"jazz-tools": patch
---

`jazz.attach(server)` no longer opens the store, so a main backend whose `remote()` core is down at start keeps running instead of crashing on a bare "fetch failed". Until the core is reachable, Jazz routes and sync sockets answer 503, the server log names the core's URL, and the next use after the backoff tries again. `attach()` takes the server types Hono's `serve()` returns (an HTTP/2 server without HTTP/1 is refused with an error, since it has no WebSocket upgrades), and Jazz's sync sockets no longer reach the server's other upgrade listeners, which kept a custom Next.js server from syncing over WebSockets.
