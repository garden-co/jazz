---
"jazz-tools": minor
---

Backend writes no longer create accounts implicitly. A user gets an account from a client sign-in, a guest link, or the new `jazz.createAccountFor(userId)`. `forReqUser` and `withAttribution` throw `NoJazzAccountError` (code `no_account`) for a user without one, and `accountFor` returns null. In Effect, `JazzBackend.forReqUser` / `withAttribution` fail with `JazzNoAccount`, and `JazzBackend` gains `createAccountFor`. A browser whose remembered user has lost their account on the core (a reset dev store) signs them in again instead of retrying sync forever.
