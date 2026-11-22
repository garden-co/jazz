---
"jazz-tools": patch
---

Query operators on JSON and bytes columns are now checked by the engine when a query is prepared, for every client (TS, thin client, Swift/Kotlin/Go), in its filters and `orderBy`, includes, union arms and a union's `orderBy`, hop seeds and nested joins, `gather` start and step filters, flat joins and history reads (aggregates aren't checked yet). JSON and bytes columns refuse `gt`, `gte`, `lt` and `lte` and can't be used in `orderBy`; JSON columns also refuse `contains`. Other column types keep everything the engine supports, and bytes columns keep `contains`, which TS used to refuse.

What TS code sees:

- An `include` ordered by a JSON or bytes column is now refused, as it already was at the root.
- `db.subscribe(...)` with a refused operator no longer throws synchronously: the error reaches the subscription's `onError` with code `query`, so a `try`/`catch` around `subscribe` won't catch it. `db.all(...)` rejects with code `query`.
- The message changed, for example from `JSON column "meta" only supports eq/ne/in/isNull operators.` to `Query: JSON column "meta" does not support "gt"; use eq, ne, in, notIn, isNull or isNotNull`.
- Swift/Kotlin/Go and thin-client queries that compared JSON as text or bytes as blobs are now refused.
