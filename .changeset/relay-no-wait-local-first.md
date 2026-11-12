---
"jazz-tools": patch
---

A local-first subscription with `firstLoadRemoteWaitMs: 0` behind a browser SharedWorker or React Native worker now opens from the worker's local store at once, instead of waiting for the server's answer. Thanks to @Xelson for the fix.
