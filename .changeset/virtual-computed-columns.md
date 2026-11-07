---
"jazz-tools": minor
---

Add `s.computed(callback)` alongside relationships in `s.table`'s second argument for virtual, read-only columns with inferred row types and automatically discovered dependencies. Computed fields use app-facing values, work in selected reads, includes, relationship hops, transactions and subscriptions, and add no stored columns. Existing `.transform({ from, to })` columns remain stored and writable.
