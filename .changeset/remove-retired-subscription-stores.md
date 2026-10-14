---
"jazz-tools": patch
---

**Breaking:** Removed the retired subscription-cache stores from Jazz's storage format. Databases declaring the old `jazz.subscription-program-fact-key.v1` codec are rejected at open instead of opening and clearing those caches. There is no automatic migration; create a fresh database and restore or resync its data. Current-format databases continue to retain native rows and pending writes across restarts.
