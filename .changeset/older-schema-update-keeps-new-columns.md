---
"jazz-tools": patch
---

Fix partial updates from clients on an older schema version resetting columns added by a later migration to their default. An update now carries forward columns its writer does not know from the row version it applied to.
