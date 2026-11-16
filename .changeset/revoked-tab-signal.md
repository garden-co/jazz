---
"jazz-tools": patch
---

A tab whose session `jazz.revokeUser` ended now signs out in the background too, not only once it is in view again: as a visible tab, at the second signed-out answer from your backend (after `onSignedOut`). Other refusals in a background tab still wait for focus. While refused, `db.getSyncStatus().link` says why: `{ state: "offline", reason: "auth", authReason: "revoked" }`. Writes made meanwhile stay pending, so an app can stop taking edits from there. A thin client now reads the refusal of an admitted session as one (before, it reconnected and was refused for a missing credential).
