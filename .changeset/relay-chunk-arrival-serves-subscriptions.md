---
"jazz-tools": patch
---

React Native subscriptions receive rows whose large values the native relay fetches from the server. Chunks that arrive at a relay serving sessions now trigger the tick's second serving pass, so the subscriptions that waited on them get their delta in the same tick instead of waiting for one that a relay ticking on demand never runs.
