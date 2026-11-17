---
"jazz-tools": patch
---

Better Auth adapter: `sortBy`, `limit` and `offset` now run in the query engine (byte order for text, declaration order for enums), instead of sorting every match in JavaScript. `where` shapes the engine cannot answer (OR connectors, case-insensitive matches, operators a column does not support) now throw `UnsupportedBetterAuthQueryError` instead of loading the whole table. `starts_with`/`ends_with` keep working.
