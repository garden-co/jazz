---
"jazz-tools": minor
---

Add `s.computed(table, fields)` for virtual, read-only columns with inferred dependency types. Computed fields use app-facing values, work in selected reads, includes, relationship hops, transactions and subscriptions, and add no stored columns. Existing `.transform({ from, to })` columns remain stored and writable.
