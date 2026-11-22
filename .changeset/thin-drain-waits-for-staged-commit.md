---
"jazz-tools": patch
---

A browser tab's upgrade from its thin client to the local-first one no longer loses a transaction committed just before it: the thin client waits for a commit whose staged writes the server is still answering, instead of closing under it with "the thin runtime is closed".
