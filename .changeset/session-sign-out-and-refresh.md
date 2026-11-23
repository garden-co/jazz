---
"jazz-tools": patch
---

**Sessions.** With `linkDeviceAccountOnSignUp: false`, `refresh()` after your own sign-in now signs a user who has no Jazz account yet in to an account of their own, as documented, and the device account stays on the device. `signOut()` now counts on the device as soon as it starts: when it can't finish because writes can't upload yet, it still rejects and the tab stays signed in so you can call it again, but a reload opens no one, and the next sign-in gets its own account. The account's key and unsynced writes stay on the device and upload when it signs in again.
