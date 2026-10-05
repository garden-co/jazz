---
"jazz-tools": patch
---

Keep subscriptions that select only small columns fast beside large values: a listing no longer downloads, or waits for, the large columns it does not select, so a client joining next to a multi-megabyte file sees its rows promptly.
