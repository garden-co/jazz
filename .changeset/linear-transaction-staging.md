---
"jazz-tools": patch
---

Make staging many rows in one transaction linear instead of quadratic: a 10k-row in-memory batch write is about 4× faster.
