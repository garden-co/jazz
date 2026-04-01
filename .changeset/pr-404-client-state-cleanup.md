---
"jazz-tools": patch
---

Self-hosted servers now clean up disconnected client state after a configurable TTL, while deferring cleanup for clients that still have unprocessed inbox entries.
