---
"jazz-tools": patch
---

Exclusive transactions no longer act on stale data. The server now checks an exclusive transaction against the rows it actually read, including counts and other aggregates, and rejects the commit with `exclusive_conflict` if any of them was deleted or changed, or if a row now matches one of its queries that it did not see (for example someone else's redemption of a single-use invite). A revoked invite can no longer be redeemed.

Exclusive transactions can now be prepared offline: reads answer from local data, the commit is stored locally, and the server accepts it once it syncs if everything the transaction read still holds.

**Upgrade clients that use exclusive transactions.** Servers now require the row records that alpha.58 clients send with each exclusive read. An exclusive transaction from an older client is rejected with `exclusive_conflict` whenever one of its queries, table reads or counts matches any rows on the server, on every attempt, until the client is upgraded. Reads of a single row by id are unaffected.
