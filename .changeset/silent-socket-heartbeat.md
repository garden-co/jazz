---
"jazz-tools": patch
"jazz-napi": patch
---

A browser client now notices a connection that went silently dead, such as a half-open TCP link or a stalled proxy, and reconnects. Before, it kept showing `connected` (or a thin client stayed `connecting`) while nothing arrived and pending writes never landed, until the page was reloaded. The server now sends browser clients a small heartbeat with its keepalive ping every 20 seconds, and a client that hears nothing at all for 45 seconds drops the link and reconnects. Native clients already saw the server's pings and are unchanged.
