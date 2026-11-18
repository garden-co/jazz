---
"jazz-tools": patch
---

When `fromJWT` can't fetch its JWKS (the identity provider is down, answers with an error, or doesn't answer within 30 seconds), createJazz now answers sign-in requests with 503 and `Retry-After: 5`. Before, it answered 500 `authenticate_failed`. Clients retry and stay signed in, as before. The backend log now names the unreachable JWKS URL, instead of saying your `authenticate` threw and needs fixing. A JWKS URL that answers with something that isn't a JWKS is still a setup error: 500, logged as before.
