---
"jazz-tools": patch
---

`fromJWT` with a `jwksUrl` no longer signs users out when your issuer rotates keys. A token signed with a key missing from a JWKS fetched in the last 30 seconds used to be refused. Now it throws, so clients retry until the next fetch picks up the key.
