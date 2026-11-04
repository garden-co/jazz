---
"jazz-tools": patch
---

A `vercel()` core claims its Blob store or S3 bucket for its Vercel environment, and a core of another environment is refused there: connect a separate store to production and to previews. The server takes the environment with `--blobs-environment`.
