---
"jazz-tools": patch
---

`fromJWT` documents how it caches a `jwksUrl`: for up to five minutes, refetched at most every 30 seconds for a token naming an unknown key. Publish a new key in your JWKS before your issuer signs with it.
