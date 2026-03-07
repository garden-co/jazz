---
"jazz-tools": patch
---

Fix: sync server now falls back to the server-established session when a `QuerySubscription` payload omits one.

Demo and anonymous auth clients sent `session: None` in subscription payloads, causing all their queries to return empty results after the payload-session change in #147. The server now prefers the session it validated from auth headers during the SSE handshake, falling back to the payload only for fully unauthenticated clients. Payload sessions that differ from the server-established session are ignored and a warning is logged.
