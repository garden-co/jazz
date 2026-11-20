---
"jazz-tools": patch
"jazz-wasm": patch
---

A Jazz core on a Cloudflare Durable Object can keep large values in R2. Bind an R2 bucket to the core's Worker as `JAZZ_BLOBS`, now the recommended setup: the bytes of large values then live in the bucket, under `jazz/<app id>/` (or `JAZZ_BLOBS_PREFIX`), and the object's SQLite keeps only their index. Every upload carries the chunk's SHA-256, so R2 rejects a corrupted one. A failed upload keeps the value in SQLite and is retried with backoff. The first core to use a prefix claims it, and any other core there is refused: its `/ready` answers 503 and names the owning app. A core that already holds large values in SQLite moves them to the bucket once the binding is added, and from then on refuses to start without the binding. Without the binding nothing changes. The `s3()` docs now say it reaches R2 only through R2's S3 API from a native server.
