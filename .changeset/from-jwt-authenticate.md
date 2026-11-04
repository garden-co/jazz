---
"jazz-tools": patch
---

`fromJWT()` from `jazz-tools/backend` is a ready-made `authenticate` for `createJazz` that signs users in with JWTs from an identity provider (WorkOS, Auth0, Clerk, any OIDC issuer). It verifies the signature (`jwksUrl` or `publicKey`), expiry, `issuer` and `audience`. It names the user by `sub` and passes the token's other claims to policies as `session.claims`. A request without a token, or with one that does not verify, is signed out.

`createJazz` now spells a UUID app id in lowercase hyphenated form in its users' `jazz:app:<appId>` issuer, so a store given the id in another spelling still opens the same accounts.
