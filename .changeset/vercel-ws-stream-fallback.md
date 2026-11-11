---
"jazz-tools": patch
---

`jazz.next()` on Vercel without the optional `ws` package now logs one clear error at startup ("next() on Vercel needs the ws package: npm install ws") and answers sync sockets with the HTTP stream signal (426 + `x-jazz-stream`) instead of a 500 per upgrade, so clients switch to the HTTP stream at once. A Node backend client (`createJazzClient({ credential })`) now falls back to its main backend's HTTP stream the way a browser does, and a socket that fails otherwise reports its upgrade's HTTP status (e.g. "answered with HTTP 500") instead of a bare "websocket transport error".
