---
"jazz-tools": patch
---

Fix the browser runtime stalling forever under webpack 5 (for example `next dev --webpack`). The broker SharedWorker was constructed from two separate `new SharedWorker(new URL(...))` call sites, so webpack emitted two worker chunks and the foreground lease and the runtime connection ended up in different SharedWorker realms: the runtime worker never loaded WASM or opened its WebSocket, and queries stayed pending. Both now go through a single construction site, so every bundler emits one broker worker.
