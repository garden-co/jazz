---
"jazz-tools": patch
---

`fromJWT()` from `jazz-tools/backend` is a ready-made `authenticate` for `createJazz` that signs users in with JWTs from an identity provider (WorkOS, Auth0, Clerk, any OIDC issuer). It verifies the signature (`jwksUrl` or `publicKey`), `issuer`, `audience` (required; `false` opts out for issuers that are specific to one app) and expiry (`exp` is required; `exp` and `nbf` allow 60 seconds of clock skew, set by `clockToleranceSeconds`). It names the user by `sub` and passes the token's other claims to policies as `session.claims`. A request without a token, or with one that does not verify, is signed out.
