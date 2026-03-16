---
"jazz-tools": patch
---

Self-hosted server now supports JWKS key rotation without a restart. Keys are cached with a 5-minute TTL and automatically refetched when a JWT arrives with an unknown key ID or a signature mismatch.
