---
"jazz-tools": patch
---

Allow ordinary mutations on rows made visible again by a restore. A retained deletion-register history no longer causes a restored row to be rejected as already deleted; rows that remain deleted still reject ordinary writes. This includes deleting a row again after either a standalone or transactional restore, without changing branch-view rules or the stored history format.
