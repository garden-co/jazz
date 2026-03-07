---
"jazz-tools": patch
---

Fix a race condition in `subscribe_internal` where the callback could be called before it was registered.
