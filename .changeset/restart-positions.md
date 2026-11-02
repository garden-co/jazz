---
"jazz-tools": patch
---

A reopened app reads the rows it kept as of the last position its server settled, and keeps naming its own writes accepted past that position, until the server settles again.
