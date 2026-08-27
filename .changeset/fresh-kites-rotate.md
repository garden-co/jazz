---
"jazz-tools": patch
---

Limit backend JWKS documents to five minutes of trust. Authentication now fails closed if the provider cannot refresh an expired document, trading provider outage availability for prompt signing-key revocation.
