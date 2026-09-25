---
"jazz-tools": patch
---

Fix a subscription over a very large table (around 100,000 rows) failing the server tick with `semantic message exceeds routed payload limit`, which left the subscriber without any rows. When a first sync would exceed the limit, the server now leaves out some row bodies and the client fetches them through the existing repair path before applying the update. Smaller updates are sent unchanged, and the wire format is unchanged.
