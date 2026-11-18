---
"jazz-tools": patch
---

Subscriptions and one-shot reads with a `limit` or `offset` now resume after a reconnect or reload like other reads: the server sends only the rows that changed or entered the window, instead of the whole window again.
