---
"jazz-tools": patch
---

Coalesce concurrent JWKS downloads per URL and start the forced-refresh cooldown only after a successful download. Freshly fetched keys no longer trigger an immediate duplicate download for an invalid JWT, while stale keys can still refresh after provider rotation. Failed downloads are not cached and remain immediately retryable.
