---
"jazz-tools": patch
---

Bugfix: close the server peers before calling the onAnonymousAccountDiscarded hook, to ensure that there won't be conflicting issues with the new context sync