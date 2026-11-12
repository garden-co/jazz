---
"jazz-tools": patch
---

**Breaking (alpha):** `createJazz`'s `authenticate` must return `expiresAt` with the user: when that answer stops holding, in Unix seconds (your session's expiry, or its token's `exp`). `fromJWT` returns the token's `exp`. A missing, non-finite or millisecond `expiresAt` throws an `AuthenticateExpiresAtError` naming the fix. `recheckSeconds` is removed (passing it throws): Jazz no longer replays the opening request from a timer, outside your framework's request scope.

Instead, a sync connection lasts until its `expiresAt`. Shortly before then, the server asks the client to connect again, and `authenticate` runs on that new request with the client's current cookies, so `cookies()`, `withAuth()` and `auth()` work. For the same user the client swaps connections without the app seeing any change. Changed claims take effect on the new connection, and a user no longer signed in is refused and signed out. `jazz.revokeUser(userId)` ends a user's open connections on this instance at once, for sign-out handlers and session-revoked webhooks. Local-first guests are let in anew every `sessionTtlSeconds`.

In the browser, the windows of one browser ask your backend who is signed in one at a time and share the answer. Before, several windows waking at once could race a single-use refresh token in auth middleware and lose the session. The docs now say to keep auth middleware off Jazz's routes. `createJazzClient({ onSignedOut })` is called before a confirmed sign-out; returning `"keep"` stays signed in.

A sign-in or sign-out that fails while its client still works (offline, a storage wipe the worker couldn't start) now leaves the session `ready` with that client and the error, instead of `error` with no client. A sign-out whose teardown already began still ends `signed-out`.
