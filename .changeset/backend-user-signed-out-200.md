---
"jazz-tools": patch
---

`createJazz` now answers a signed-out `POST …/accounts/backend-user` (the probe a session sends to ask your backend who is signed in) with `200` and `{ "user": null }` (`Cache-Control: no-store`) instead of `401 signed_out`, so a signed-out or guest page load no longer logs a red 4xx in devtools. Clients read both answers as signed out, so alpha.60/61 clients keep working against this backend and this client keeps working against older ones. `backend-sign-in`, `backend-link` and `backend-link/challenge` still answer `401 signed_out`. If you call `backend-user` yourself, or alert on its 401s, read a 200 without `identity` as signed out; backends that count 4xx responses will see fewer of them.
