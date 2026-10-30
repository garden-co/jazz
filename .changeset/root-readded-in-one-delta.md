---
"jazz-tools": patch
---

Fix nested data that stopped updating after a subscription delta removed a root row and added it back in the same frame (for example, a joined or `include` result that re-sorted). The client kept the row in the result but dropped its nested state, so later changes to its children were never applied. This also affected alpha.56.
