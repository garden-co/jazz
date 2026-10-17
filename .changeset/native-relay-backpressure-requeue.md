---
"jazz-tools": patch
---

Fix the React Native relay dropping outbound sync messages when its socket was backpressured. Unsent messages now stay queued in order and are retried, and the socket worker keeps running instead of stopping.
