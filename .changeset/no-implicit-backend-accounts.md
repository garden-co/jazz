---
"jazz-tools": patch
---

Backend writes no longer create accounts implicitly. A user gets an account from a client sign-in, a guest link, or the new `jazz.createAccountFor(userId)`. `forReqUser` and `withAttribution` throw `NoJazzAccountError` (code `no_account`) for a user without one, and `accountFor` returns null.
