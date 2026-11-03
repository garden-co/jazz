---
"jazz-tools": patch
"@jazz/rust": patch
---

A `vercel()` core now keeps large values in the project's Vercel Blob store by default once one is connected (when `BLOB_STORE_ID` is set). You can also choose it with `vercel({ blobs: vercelBlob() })`, or run `jazz-tools server --blobs vercel-blob`. The core writes and reads every blob itself, through the app's `/jazz` route, using the function's own OIDC token. The Sandbox holds only the store's id. Blobs are private. Each one sits under `jazz/<appId>/<store id>/` (claimed by one core) and is checked against its SHA-256 when read, because Vercel Blob keeps no checksum. A core without a Blob store keeps large values in its SQLite file and says how to connect one. It moves to the Blob store as soon as one is connected. `s3()` remains for buckets.
