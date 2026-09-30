---
"jazz-tools": patch
---

Exclusive transactions no longer act on stale data. The server now checks an exclusive transaction against the rows it actually read, including counts and other aggregates, and rejects the commit with `exclusive_conflict` if any of them was deleted or changed, or if a row now matches one of its queries that it did not see (for example someone else's redemption of a single-use invite). A revoked invite can no longer be redeemed.

Exclusive transactions can now be prepared offline: reads answer from local data, the commit is stored locally, and the server accepts it once it syncs if everything the transaction read still holds.

**Upgrade clients and backends that use exclusive transactions.** Only alpha.58 clients send the row records the server checks, so a backend that redeems invites, like the invite links recipe, is protected only once it runs alpha.58. An exclusive transaction from an older client is rejected with `exclusive_conflict`, on every attempt until the client is upgraded, whenever one of its reads (a query, a read by id, a table read or a count) matches rows on the server, and whenever it updates or upserts a row in a table that holds other rows the writer can read. An older client's read is checked only against rows that still exist, so a read of a row that was deleted since, such as a revoked invite, can still commit. Inserts and deletes without reads still commit.

Exclusive conflicts rely on the writing client reporting its reads, so they are not a security boundary. Enforce rules every writer must follow in permission policies, or write on your backend.
