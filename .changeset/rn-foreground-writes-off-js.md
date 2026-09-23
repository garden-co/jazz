---
"jazz-tools": patch
---

React Native writes no longer block the JS thread on applying the write, subscription (IVM) evaluation or relay pumping: the native relay owner does that work on its own thread and wakes JS with already-computed results. Failures the resident local state already decides still throw synchronously; others surface through the write handle and `onMutationError`, as on web and Node. Direct writes are bounded per client with backpressure, and reads and subscriptions opened after a write in the same turn reflect it.
