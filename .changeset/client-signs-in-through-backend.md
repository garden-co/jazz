---
"jazz-tools": patch
---

Clients no longer sign in to Jazz with a JWT. Sign-in goes through the app's backend: `createJazz({ authenticate })` reads the app's own session, and the client calls `accounts.loginWithBackend()` (or `backendAuth()` / `betterAuth()` in the framework bindings). A local-first guest keeps its data by calling `accounts.linkWithBackend()` after signing in. `registerJWT`, `loginJWT`, `loginOrRegisterJWT` and the `jwtAuth` helpers are removed from the public client API.
