---
"jazz-tools": patch
---

A write built on another write that the server rejected is now rejected too, with the new reason `predecessor_rejected` (code `write_rejected`). Before, it was applied over the server's row and brought back the values of the rejected write. This includes a delete made after a rejected edit of the same row: retry it once the row shows the server's version.

App columns can no longer be named `rid`, `_b`, `_seq`, `_st`, `_base` or `_cols`: those names are taken by the store's own row columns.
