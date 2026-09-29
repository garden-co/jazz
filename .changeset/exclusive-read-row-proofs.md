---
"jazz-tools": patch
---

An exclusive transaction can no longer commit on top of a row the server already deleted or changed. Before, a backend could read a revoked row from its local replica (for example a deleted invite), and a retry after `exclusive_conflict` could be accepted anyway. Rows returned by queries inside an exclusive transaction are now checked row by row on the server.
