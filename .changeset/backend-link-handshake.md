---
"jazz-tools": patch
"jazz-napi": patch
---

A backend's own `jazz.db` no longer loses its link when the backend does a burst of synchronous work right after the link opens. Inserting 20,000 rows in a loop held the event loop for several seconds, so the link's handshake missed the server's 2 s deadline and the first read after the inserts failed with "websocket closed". The link from `jazz.db` to the server in the same process now has no handshake deadline; links from the network keep it.
