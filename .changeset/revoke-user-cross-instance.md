---
"jazz-tools": patch
"jazz-napi": patch
---

`jazz.revokeUser` now ends a user's sessions on every backend instance, not only on the one that runs it. On serverless hosts (Vercel and the like) the sign-out handler usually runs on a different instance from the user's sockets and HTTP streams. The revocation is now recorded at the core, which closes the user's live sessions on any instance, idle ones included. It also refuses sessions that any instance admits later on an `authenticate` answer from before the call, such as a sign-in still in flight or an HTTP stream's reused answer. A sign-in that starts after the call works as before. `revokeUser` still resolves with the number of sessions it ended on the calling instance.

**Upgrade the core before backends to get cross-instance revocation.** A newer backend in front of an older core warns once and ends only its own instance's sessions, as before. An older backend in front of a newer core is unaffected.

- **Rollback:** an older core opens the same store. The revocation marks live in a new table (`_jazz_host_account_session_revocations`) that an older core never reads. After a rollback, recorded revocations stop applying, and sessions on other instances end at their `expiresAt` again.
- **Clients:** no client changes. The backend ends a revoked session with the same refused close it already sends for `revokeUser`, so every client that handles that close today handles this one.
