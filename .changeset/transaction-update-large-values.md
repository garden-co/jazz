---
"jazz-tools": patch
---

Fix `db.transaction` updates on rows that hold a large text or bytes value in another column. Updating one column inside a transaction no longer fails with "callers must author logical scalar values, not physical large descriptors"; untouched large values are kept unchanged, and reads inside the transaction see the update.
