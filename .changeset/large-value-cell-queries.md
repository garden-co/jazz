---
"jazz-tools": patch
---

Queries no longer read the content of `string`, `bytes` and `json` values over 64 KB. Such a value now compares by identity: `eq`, `ne` and `in` never match it against a literal of 64 KB or less, a filter literal over 64 KB is rejected as not supported yet, and two columns, joins and `group by` treat two such values as equal only when they are the same stored value. `contains` and range filters never match it, and in ascending order it sorts before every shorter value, in no meaningful order among other long values. Values of 64 KB or less filter and sort exactly as before. Reads still return the full value. In exchange, every query is plain SQL that never writes, and indexes on large-capable columns no longer carry a hidden sort-key column.
