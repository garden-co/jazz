---
"jazz-tools": patch
---

A visible tab whose sync connection is refused (after `jazz.revokeUser`, or a credential the server no longer accepts) now signs its user out at the second signed-out answer from your backend, about two seconds after the first, instead of waiting another 30 seconds. Until then its sync status reads `offline: auth`. `onSignedOut` is still asked first, and a background tab still waits for focus before signing out.

A thin client signing out while the server refuses its sign-in no longer hangs in `transitioning` when it has a write waiting to upload. Waiting for writes before the sign-out now fails as soon as the server refuses the sign-in, since nothing renews the credential of a client that is stopping. The sign-out then goes through and logs how many writes it dropped, and those writes end with `ThinRequestNotSentError`, whose message says the server refused the sign-in (not a timeout). `db.waitForSync()` still waits through a refusal, since the app may sign the user in again.

The same holds when a session moves on from a refused client in other ways: signing a user in again, switching users, or replacing the client no longer fails or leaves the old client open because its writes can't upload. Jazz stops it and logs how many writes it dropped (or kept on the device).
