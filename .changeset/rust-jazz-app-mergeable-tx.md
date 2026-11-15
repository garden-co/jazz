---
"jazz-tools": patch
---

**Rust: mergeable and serializable transactions, as in TS.**

- **`jazz.transaction(…)` is now mergeable,** like TS's `transaction`. Its writes commit together, each merging with concurrent writes, and its reads see local data together with its own staged writes.
- **`jazz.serializable_transaction(…)`** is TS's `serializableTransaction`. The server refuses its commit when anything it read or wrote changed in the meantime.
- **`JazzClient::begin_mergeable_transaction`** sits beside `begin_transaction`, which stays serializable.
