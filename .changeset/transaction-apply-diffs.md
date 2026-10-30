---
"jazz-tools": patch
---

`tx.update(..., { applyDiffs })` now works inside mergeable and exclusive transactions. Diffs address the transaction's own view of the row, so repeated appends compose, and each one rewrites only the pages it touches, so streaming into a long text value in transactions stays cheap as it grows.
