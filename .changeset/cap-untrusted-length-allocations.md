---
"jazz-tools": patch
"jazz-napi": patch
---

Reject an oversized LZ4 frame on the pre-authentication WebSocket handshake before decompressing it, closing an unauthenticated decompression-bomb that could exhaust server memory. The inbound WebSocket message-size limit is also pinned explicitly.
