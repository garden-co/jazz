---
"jazz-tools": patch
---

Live queries re-run less often when a table their read policies check changes. A membership policy like `chat_members.exists.where({ chatId: chat.id, userId: session.user_id })` now wakes a subscription only for membership rows that are (or were) the reader's own, or that change a column the policy reads, instead of on every membership write. People and other tables read through `INHERITS` wake only when a column their policy reads changes. Grants and revocations still show at once. On a 100-person chat server, a membership write re-runs about 6 room lists instead of all 100. Policies the analysis cannot bound (relation `exists(...)` with joined hops, update or delete stamps, recursive inheritance) and branch views keep re-running on every write to the tables they read.
