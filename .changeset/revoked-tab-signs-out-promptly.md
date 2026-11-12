---
"jazz-tools": patch
---

A visible tab whose sync connection is refused (after `jazz.revokeUser`, or a credential the server no longer accepts) now signs its user out at the second signed-out answer from your backend, about two seconds after the first, instead of waiting another 30 seconds. Until then its sync status reads `offline: auth`. `onSignedOut` is still asked first, and a background tab still waits for focus before signing out.
