---
"jazz-tools": patch
"jazz-napi": patch
---

`createJazz` names users `(jazz:backend, userId)` instead of `(jazz:app:<appId>, userId)`: a core admits only its own app's backend, so the issuer needs no app id. Pages that tell backend-signed-in accounts from local-first guests compare `account.identity.issuer` with `"jazz:backend"`. The alpha.59 import links returning users' old accounts to `(jazz:backend, sub)`, and keeps the alpha.59 app id in the new store's `app-id` file, so `sqlite({ path })` runs as that app, and its guests keep their accounts, without the app naming the id. A bucket or Blob store's default prefix is now `jazz/<store id>/`.
