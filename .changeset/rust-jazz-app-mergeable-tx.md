---
"jazz-tools": patch
---

**Breaking (Rust): `jazz.transaction(…)` is now mergeable, as TS's `transaction` is.** Where you relied on the commit being refused when something it read changed, switch to `jazz.serializable_transaction(…)`.

- **`jazz.transaction(…)` is now mergeable,** like TS's `transaction`. Its writes commit together, each merging with concurrent writes, and its reads see local data together with its own staged writes.
- **`jazz.serializable_transaction(…)`** is TS's `serializableTransaction`. The server refuses its commit when anything it read or wrote changed in the meantime.
- **`JazzClient::begin_mergeable_transaction`** sits beside `begin_transaction`, which stays serializable.
