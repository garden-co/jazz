---
"jazz-tools": patch
"jazz-rn": patch
---

React Native: when the server refuses a session's bearer (for example one that expired while the app was in the background), the native socket no longer retries it forever. It stops and reports the refusal, which reaches the app as an auth failure. The account then renews its token and reconnects, instead of the app crashing on an uncaught tick error. Other connection refusals (TLS, a proxy refusing the upgrade) are still retried.

An account now renews on any refused credential except a disabled account, including one the server calls invalid, as after a signing key rotation. A renewal that hands back an already-expired token renews again, under the usual backoff.
