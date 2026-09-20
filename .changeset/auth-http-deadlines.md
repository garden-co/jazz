---
"jazz-tools": patch
---

Bound account registry and backend JWKS requests, including response bodies, to thirty seconds so stalled authentication cannot indefinitely block session teardown. Failed JWKS downloads remain immediately retryable without extending cached key trust.
