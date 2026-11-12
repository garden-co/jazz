---
"jazz-tools": patch
---

A backend now stops on SIGTERM or SIGINT while browsers are connected. Hosts shut down by closing their server and waiting for the connections it still serves (`next start`, an Effect runtime, `server.close()` in your own handler), and Jazz's sync sockets and HTTP streams stayed open until the browsers left: the old process lingered, kept its `sqlite()` store locked, and the restarted one refused to open it. On those signals every open `createJazz` now closes, ending its clients' sockets and streams with "server shutting down" so they reconnect to the next instance. When nothing else handles the signal, the process then stops as it would have; otherwise your handler decides. `jazz.close()` now also ends HTTP stream responses and accepted sockets itself, instead of leaving them to the client.
