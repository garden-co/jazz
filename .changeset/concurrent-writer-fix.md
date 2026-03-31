---
"cojson": patch
---

Fix InvalidSignature errors when loading from storage with a concurrent writer

The storage load path read session metadata and transaction rows in separate
non-transactional queries. The transaction query used `idx <= lastIdx` where
`lastIdx` is a count, not an index. When a concurrent writer committed a new
transaction at that index between the two reads, the query picked up the extra
row and paired it with a signature that didn't cover it.

Convert `lastIdx` to an index (`lastIdx - 1`) so the query matches the
convention used by `signatureAfter` entries. This ensures a stale session read
never overshoots the signature boundary, even under concurrent writes.
