---
"jazz-tools": patch
---

Keep a new Cloud CLI login intact when an older logout finishes. Logout now removes and revokes the same captured session, including when its access token needs refreshing.
