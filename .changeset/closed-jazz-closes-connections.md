---
"jazz-tools": patch
---

`next start` now stops on SIGTERM while browsers are connected. Next waits for its keep-alive connections, and a browser re-opening its sync stream to the closed Jazz kept one busy, so the process stayed up with its port bound and logged "this Jazz was closed" on every retry. Once a Jazz has closed, its routes and sync sockets answer 503 with `Connection: close` (on HTTP/1; HTTP/2 has no such header) and log nothing, so the connection ends and the browser reconnects to the next instance. A store that can't open keeps its connections.
