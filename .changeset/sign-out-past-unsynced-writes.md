---
"jazz-tools": patch
---

Signing out, or being signed out by the backend, no longer fails once the server refuses this user's sign-in (their backend session ended). Before, a user whose session ended with a write still on the device was stuck on "Graceful shutdown could not synchronize pending writes; the context remains open", through Retry, sign-out and reloads, with no way back to sign-in. Jazz now signs out and warns about the refused writes: a persistent client keeps them on this device and uploads them when that account signs in again, and a client that keeps nothing says it dropped them. A sign-out during an outage still fails, so the app can retry once the writes can upload.
