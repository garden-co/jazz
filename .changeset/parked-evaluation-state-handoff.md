---
"jazz-tools": patch
---

Fix duplicate and lost query results when a write arrives while an earlier write is still waiting for large-value content. The later write now sees the earlier write's finished query state. Before, a joined row could be reported twice, and a reference from the later write could be dropped.
