---
"jazz-tools": patch
---

Fix branch-view reads so a head row that an older schema cannot show still hides the base row, and so tables whose branch column is an enum read back their branch values instead of failing with a type error.
