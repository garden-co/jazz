---
"cojson": patch
"cojson-transport-ws": patch
---

Improved sync load handling and queue behavior by prioritizing pending loads and ensuring peers always respond to load requests, including cases with no new content.

Added queue and in-flight load metrics, plus richer WebSocket peer metadata and ping-delay logging to improve observability during sync operations.
