---
"jazz-tools": patch
---

Avoid repeatedly rebuilding and hashing unchanged schema catalogues when serving sync messages. Catalogue changes and reconnects continue to send current metadata, and failed sends remain retryable.
