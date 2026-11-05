---
"jazz-tools": patch
---

createJazz: backend clients. A machine outside your main backend can sync through its `/jazz` with its platform's OIDC token. List each trusted workload in `backendClients` with `trustWorkload({ issuer, audience, claims, as })`. With `as: "backend"` it is a trusted machine and syncs with the main backend's authority. With a function from the token's claims to a user it is a scoped machine and acts as that user, with that user's permissions. In Node, pass the token, or a function that returns a fresh one, as `createJazzClient({ credential })`: the session opens as whatever the main backend trusts the token as. createJazz no longer forwards a request's `Authorization` header to the core.
