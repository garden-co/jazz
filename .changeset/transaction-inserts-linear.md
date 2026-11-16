---
"jazz-tools": patch
---

Staging many writes in one transaction now costs time proportional to the number of writes: drawing a row id for a staged insert and repeating a session's unchanged claims no longer undo the transaction's applied writes, which made each write replay every earlier one (1,600 inserts took ~10 s, now ~0.2 s).
