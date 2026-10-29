---
"jazz-tools": patch
---

A relay (a browser worker or a native relay) now refuses a session of another identity with the error reason `identity_mismatch`. Before, it uploaded that session's writes under its own identity, so the server attributed them to the relay's user. The shipped setups always serve the relay's own identity, so they are unaffected.
