---
"jazz-tools": patch
---

A trusted backend client can act for a user: `client.forUser(userId)` on a `createJazzClient` from `jazz-tools/backend`, opened with a credential the main backend trusts as `"backend"`, returns the database as that user, like the main backend's `jazz.forReqUser(request)`. Writes carry the user's identity and are checked against their permissions. A user with no account yet gets a `NoJazzAccountError`.
