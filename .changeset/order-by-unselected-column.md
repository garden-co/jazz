---
"jazz-tools": patch
---

Fix one-shot reads (`db.all`, `db.one`) that order by a column they don't `select`: rows now come back in that column's order instead of row-id order, including for `limit`/`offset` pages. Subscriptions were already ordered correctly.
