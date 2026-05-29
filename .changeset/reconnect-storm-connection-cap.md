---
"jazz-tools": patch
---

Bound the reconnect-storm amplification on the server's WebSocket path: enforce a per-`client_id` connection cap (4) with evict-oldest semantics so a single client_id cannot pin unbounded fan-out memory, and time out pre-handshake sockets after 10s so an unauthenticated peer cannot park resources without sending the `AuthHandshake` frame. Evicted clients receive a `RateLimited` error frame followed by a policy close.
