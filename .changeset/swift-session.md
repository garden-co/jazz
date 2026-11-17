---
"jazz-tools": patch
---

Jazz for Swift has `JazzSession`, as TS's: it follows who is signed in (a device account, your sign-in's bearer token, or no one), hands out a `Client` per user (`client()`, the same until `clientEpoch` changes), and has `snapshots`, `refresh()`, the device-account calls, `signOut()`, `retry()` and `close()`. `JazzError` gains `.replaced` (a client kept past a change of user) and `.noUser` (no one signed in); code switching over every `JazzError` case needs the two new cases.
