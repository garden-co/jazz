---
"jazz-tools": patch
---

Sync status now reads `offline` (reason `network`) when the server takes the connection but never answers it, for example a dev proxy holding the WebSocket upgrade for a backend that is down. A connection attempt now gives up when the server's hello hasn't arrived within 10 s, so the link retries and reports the outage. Before, it read `connecting` for as long as the attempt hung.
