---
"jazz-tools": patch
---

On Vercel, `jazz.next()` now closes each sync WebSocket (1012, reconnect) shortly before its function's deadline, the route's `maxDuration`, preferring a moment with no traffic. Vercel ends the socket at that deadline and stops answering on it about 15 s before it closes; clients used to sit through that stall, and requests sent in it were lost. Now they reconnect at once.
