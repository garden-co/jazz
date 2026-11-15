---
"jazz-tools": patch
---

A query no component uses any more (a page you navigated away from) no longer stays subscribed for 30 seconds. After one second unused, it stops being re-evaluated and refreshed on every write, but keeps its last result for the rest of the 30 seconds. Navigating back still shows those rows at once while the query resubscribes and catches up. A Suspense read that is still waiting for its first result stays subscribed until it gets one.
