---
"jazz-tools": patch
---

A thin client reading a large value whose bytes live only in an `s3()` (or other network) blob store, for example after the server restarted, no longer hangs the server. Before, the read waited for a fetch the busy server could never finish, and every later request timed out until a restart. Now the request waits while the server fetches the bytes in the background, then answers; other clients are served meanwhile, even when the bucket is slow or down. If the bucket fails, the read answers with the storage error instead of waiting, and later reads fail fast until the server's retry backoff ends. Reads inside a transaction and large-value writes go the same way.
