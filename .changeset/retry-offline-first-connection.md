---
"jazz-tools": patch
---

Retry a first server connection that fails at the network layer, so a local-first client opened while offline syncs its writes once the network returns. A server URL that cannot be reached now reports its transport error after 7.5 s of retrying instead of immediately.
