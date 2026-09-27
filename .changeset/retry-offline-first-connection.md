---
"jazz-tools": patch
---

Retry a first server connection that fails at the network layer, so a local-first client opened while offline connects and syncs its pending writes once the network returns, instead of staying local-only until it is reopened.
