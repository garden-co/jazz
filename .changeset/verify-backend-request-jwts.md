---
"jazz-tools": patch
---

Verify bearer JWTs inside backend `createJazzContext(...).forRequest()` / `withAttributionForRequest()`, add backend `jwksUrl` and `allowSelfSigned` config, and share JWT-to-session mapping with the runtime session helpers. These request-scoped backend helpers are now async so callers can await self-signed or JWKS-backed verification.
