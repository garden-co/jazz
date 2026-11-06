---
"jazz-tools": patch
"jazz-napi": patch
---

A web or Node client now notices a connection that went silently dead, such as a half-open TCP link or a stalled proxy, and reconnects. Before, it kept showing `connected` (or a thin client stayed `connecting`) while nothing arrived and pending writes never landed, until the page was reloaded. The server now sends these clients a small heartbeat with its keepalive ping every 20 seconds, and a client that hears nothing at all for two minutes drops the link and reconnects. Clients on the native transport (React Native, and backends using `remote()`) don't have this yet.
