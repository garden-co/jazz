---
"jazz-tools": patch
---

A page's copy of the stream-endpoint memory now keeps IndexedDB's answer for each endpoint, "stream" or "no stream". An endpoint with nothing kept, for example because storage is full, is looked up in IndexedDB as before. A page with full Web Storage no longer fails a socket on every load on hosts that can't upgrade sockets. A page also no longer misses an endpoint that only IndexedDB knows, such as one learned before an upgrade or by a worker. Its "no stream" answer is re-checked every 10 minutes, and expired entries are removed.
