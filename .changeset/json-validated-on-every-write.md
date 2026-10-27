---
"jazz-tools": patch
---

JSON columns are now checked against their declared schema on every write path, in the engine itself: local inserts, updates and upserts, transaction writes, and writes the server applies for a client. Values large enough to be stored in chunks are checked whole, like any other. A value that does not match, or text that is not JSON, is refused with a `WriteRejected` error that names the column. Only values a write authors are checked: moving a row between branches carries its stored values as they are, even if the column's schema has been tightened since they were written.
