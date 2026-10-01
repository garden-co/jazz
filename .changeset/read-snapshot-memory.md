---
"jazz-tools": patch
---

Lower a sync server's peak memory while it serves a large query to a fresh client. The first result is no longer copied before it is sent, is encoded once at its exact size, and no longer holds oversized query buffers.
