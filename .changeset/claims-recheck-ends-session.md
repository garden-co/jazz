---
"jazz-tools": patch
---

`createJazz`'s `recheckSeconds` now compares a signed-in user's claims too, not just their user id. When `authenticate` names the same user with different claims (a role changed), their open sync sessions end with "authenticate again" and the client reconnects with the new claims, so policies stop applying the old ones within `recheckSeconds` (60 s by default) rather than at `sessionTtlSeconds` (an hour). Claims compare as JSON with object keys sorted; return claims that only change when the user's access does.
