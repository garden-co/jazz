---
"jazz-tools": patch
---

A write to a table read by an ordered subscription (`orderBy`) no longer costs time proportional to the whole ordered result. The engine now tracks row positions only for outputs that can use them. Results are unchanged.
