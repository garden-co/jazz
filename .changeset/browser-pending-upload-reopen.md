---
"jazz-tools": patch
---

Fix browser startup freezing when restoring pending uploads requires a cold IndexedDB read. Recovery now yields to asynchronous storage instead of blocking its callbacks.
