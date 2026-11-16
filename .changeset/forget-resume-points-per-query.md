---
"jazz-tools": patch
---

When a client loses a cached row (access lost, a listing evicts it, or it needs the row resent whole), only the open subscriptions that held that row now lose their resume point. Before, every saved resume point was dropped, so the next reconnect reloaded every query in full.
