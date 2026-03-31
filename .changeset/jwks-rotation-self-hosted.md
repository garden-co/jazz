---
"jazz-tools": patch
---

Self-hosted server now supports JWKS key rotation without a restart. Keys are cached with a configurable TTL (5 minutes by default, override with `JAZZ_JWKS_CACHE_TTL_SECS`) and automatically refetched when a JWT arrives with an unknown key ID or a signature mismatch. A 10-second cooldown prevents forced refreshes from being abused as a DoS vector. If the JWKS endpoint goes down, the server continues validating against the stale cached keyset.
