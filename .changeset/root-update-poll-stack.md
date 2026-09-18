---
"jazz-tools": patch
---

Isolate branch-view update preparation from the async poll frame used by ordinary root-table updates. Root writes can synchronously refresh resident subscriptions without carrying branch-only preparation through that call stack. Branch visibility, write authorization, inherited cells, and authored-column semantics remain unchanged; no storage or wire encoding changes are involved.
