---
"jazz-tools": patch
---

A Node client no longer hangs when its upstream connection is established while a local read is suspended between evaluation turns. Synchronous binding calls now advance suspended reads instead of spinning on the state those reads hold.
