---
"jazz-tools": patch
"jazz-napi": patch
---

Fix large global query and subscription snapshots dropping rows by sequencing sync delivery and delaying `QuerySettled` tier unlocks until earlier sync updates have been applied.
