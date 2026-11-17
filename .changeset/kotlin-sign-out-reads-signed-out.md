---
"jazz-tools": patch
---

jazz-kotlin: `signOut()` leaves `session` reading `SignedOut`, even when jazz-ffi ends the session watch before it delivers the final `signedOut` state.
