---
"jazz-tools": patch
---

Wait for IndexedDB persistence before acknowledging a browser write as locally durable. Keep storage failures visible through later flushes and reopen attempts until an explicit successful reset.

Hold a durable worker's downstream acknowledgements until persistence completes, so in-memory tabs receive the same guarantee. Worker failures reject pending and subsequent Local waits; a remote server outage still permits Local durability through a healthy worker.
