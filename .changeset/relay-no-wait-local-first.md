---
"jazz-tools": patch
---

A local-first subscription from a tab behind a browser SharedWorker or React Native worker now opens from the worker's local store at once, as `durability-tiers` documents. Before, it waited for the server's answer, with no time limit, while the worker's server link was up or connecting. This covers both `firstLoadRemoteWaitMs: 0` and a plain `tier: "local-first"` read, which reach the worker as the same read; reads with a non-zero first-load wait, and remote reads, still wait for the server. Thanks to Xelson for the fix.
