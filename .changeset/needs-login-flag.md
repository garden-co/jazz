---
"jazz-tools": patch
---

Add `needsLogin` to the session and app snapshots (and `useJazzAuth()`), true while a browser reload has reopened an external-JWT account that has not logged in again and therefore does not sync. Apps without managed auth can check it to call `loginJWT` or `loginOrRegisterJWT`, and development builds warn when such an app keeps writing to that account without logging in.
