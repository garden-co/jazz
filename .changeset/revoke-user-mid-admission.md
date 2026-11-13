---
"jazz-tools": patch
---

`jazz.revokeUser(userId)` now also refuses a sync session that was still signing in when it ran (a tab reconnecting, or re-authenticating at its `expiresAt`). Before, such a session could be admitted on an `authenticate` answer read before the app ended the user's auth session, and stay connected as the revoked user, uploading their writes, until its `expiresAt`.
