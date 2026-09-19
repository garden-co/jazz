---
"jazz-tools": patch
---

Fix permission-scoped inserts and updates of JSON larger than 64 KiB. Policy candidates preserve JSON scalar types without loading large payloads that ownership checks do not inspect.
