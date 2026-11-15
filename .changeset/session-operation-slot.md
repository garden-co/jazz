---
"jazz-tools": patch
---

The session now runs one operation at a time, with explicit rules for what may start while another runs. `signOut()` takes over from a sign-in, link or `refresh()` still running: they reject with the new `JazzSessionOperationError` (`reason: "superseded"`), and nothing they finish later signs anyone back in (a refresh whose answer arrived after a sign-out used to sign the user in again). Sign-ins and `refresh()` during a sign-out reject with `reason: "signing-out"`; a sign-in during another one rejects with `reason: "pending"` (same message as before). A `refresh()` asked during a sign-in or link waits for it, and refreshes asked while one runs share one more.

A sign-out that can't upload the user's last writes (offline) no longer fails and leaves the user signed in: it signs out on this device, `signOut()` resolves, and the session reports the new status `"signing-out"` until those writes upload (tried again with backoff, on reconnect, or by `retry()`), then `"signed-out"`. Providers show their `signedOut` view meanwhile.
