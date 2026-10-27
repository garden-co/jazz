---
"jazz-tools": patch
"jazz-rn": patch
---

React Native: when the server refuses a session's bearer (for example one that expired while the app was in the background), the native socket no longer retries it forever. It stops and reports the refusal, which reaches the app as an auth failure. The account then renews its token and reconnects, instead of the app crashing on an uncaught tick error. A renewal that hands back an already-expired token renews again right away.
