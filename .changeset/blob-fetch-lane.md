---
"jazz-tools": patch
---

Reads of large values in a network blob store (S3 or Vercel Blob) no longer wait behind uploads. The leaves a read needs are fetched beside the upload batch that is out, so a read no longer waits for megabytes of uploads to land, or for the backoff after a failed put.
