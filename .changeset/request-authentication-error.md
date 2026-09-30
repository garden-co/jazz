---
"jazz-tools": patch
---

`resolveRequestSession` now throws a `RequestAuthenticationError` (exported from `jazz-tools/backend`, with `isRequestAuthenticationError` to match it across bundled copies) when the request's credential is missing, malformed, unverifiable, expired, for another issuer or audience, or refused by the account registry. Server-side failures, such as an unusable `jwksUrl` or `jwtPublicKey`, a JWKS or registry outage, or a malformed registry answer, stay plain errors, so routes can answer 401 only for the former. A JWKS refresh that fails after a bad signature is now reported as the fetch failure instead of as an invalid JWT.
