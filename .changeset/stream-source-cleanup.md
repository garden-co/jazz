---
"jazz-tools": patch
---

Abort failed streaming uploads without waiting for application source cleanup.
Release browser reader locks and request unfinished iterator cleanup while
preserving the original upload or source error.
