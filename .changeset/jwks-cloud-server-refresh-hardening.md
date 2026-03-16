---
"jazz-cloud-server": patch
---

JWKS refresh hardening: 10-second cooldown on forced refreshes prevents DoS via fabricated key IDs. Stale-if-error fallback keeps valid users authenticated during transient IdP outages. Cache TTL is now configurable via `JAZZ_JWKS_CACHE_TTL_SECS` (defaults to 300s).
