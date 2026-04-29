---
"jazz-tools": patch
---

Add new `Db.transaction` and `Db.batch` methods. These methods receive a callback as parameter and commit automatically once the callback finishes running. They are preferred over their `beginTransaction`/`beginBatch` counterparts.
