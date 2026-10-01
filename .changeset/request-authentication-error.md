---
"jazz-tools": patch
---

`resolveRequestSession` now throws a `RequestAuthenticationError` (exported from `jazz-tools/backend`, with `isRequestAuthenticationError` to match it across bundled copies) when the request's credential is missing, malformed, unverifiable, expired, for another issuer or audience, signed with an algorithm the configured `jwtPublicKey` does not support, or refused by the account registry with one of its rejection codes. Server-side failures, such as an unusable `jwksUrl` or `jwtPublicKey`, a JWKS or registry outage, a registry answer without a rejection code (such as a bare 404 for a wrong app id or registry URL), or a malformed registry answer, stay plain errors, so routes can answer 401 only for the former. A JWKS refresh that fails after a bad signature is now reported as the fetch failure instead of as an invalid JWT. The configured `jwtPublicKey` is now imported once for every algorithm it supports.
