---
"jazz-tools": patch
---

JSON columns are now checked against their declared schema on every write path, in the engine itself: local inserts, updates and upserts, transaction writes, and writes the server applies for a client. A value that does not match, or text that is not JSON, is refused with a `WriteRejected` error that names the column.
