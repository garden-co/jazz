---
"jazz-tools": patch
---

jazz-ffi sessions: a `Session` (C `jazz_session_*`) over jazz-app's `JazzSession` follows who is signed in (the app's bearer, a device account, or no one) and hands out a handle for that user, which fails `Replaced` once the user changes. Snapshots as TS's, a session watch, device accounts, sign-out and retry. New contract 2 error kinds: `replaced` and `noUser`.
