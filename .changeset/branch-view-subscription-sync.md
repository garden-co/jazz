---
"jazz-tools": patch
---

Keep branch-view subscriptions in their view across sync. A client's live subscription through a branch view now gets that view's own result from the server, its limit and ordering included, instead of every branch's matching rows. A view over a frozen base keeps its head live from the server. Writes to branches a view does not read no longer re-run its subscriptions.
