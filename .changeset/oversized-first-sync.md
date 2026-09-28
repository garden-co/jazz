---
"jazz-tools": patch
---

Fix a server failing to sync a subscription over roughly 100k rows with `semantic message exceeds routed payload limit`. When a view update would exceed the 256 MiB message limit, the server now leaves some row bodies out and the client fetches them before applying the update in one piece. Updates under the limit are sent unchanged.
