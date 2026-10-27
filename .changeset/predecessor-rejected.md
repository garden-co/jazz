---
"jazz-tools": patch
---

A write built on another write that the server rejected is now rejected too, with the new reason `predecessor_rejected` (code `write_rejected`). Before, it was applied over the server's row and brought back the values of the rejected write.
