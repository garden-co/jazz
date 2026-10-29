---
"jazz-tools": patch
---

Keep branch-view subscriptions in their view across sync. A client's live subscription through a branch view now gets that view's own result from the server, its limit and ordering included, instead of every branch's matching rows. A view over a frozen base keeps its head live from the server. Writes to branches a view does not read no longer re-run its subscriptions. The server also sends the draft's version of each base row the view's query matches, so rows the draft changed or deleted are masked even when another read brought their base version. When the draft changes a row so that you may no longer read it, the row is hidden in the draft view instead of showing the main version it replaced, and it reappears once you may read the draft's version again.

Each existing live branch-view subscription opens fresh once after upgrading, as its stored resume point is now keyed by its view too.
