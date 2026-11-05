---
"jazz-tools": patch
---

Keep a browser or native session's published states consistent while it follows your backend. A device now remembers the user your backend signed in (their account, identity and claims, never a credential) and reopens them on reload, offline too, instead of starting a fresh guest with `initial: "local-first"` each time; their claims refresh when the backend's first answer differs. Switching from one backend user to another is one transition, with no `signed-out` in between. A sign-out confirmation only signs out the account the check started with, so an app that continued as a guest meanwhile keeps it. A 404 from the backend-user route is retried with backoff and stops the watch only when it persists, and `reauthenticate()` rejects instead of resolving once the watch has stopped.
