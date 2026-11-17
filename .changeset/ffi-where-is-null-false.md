---
"jazz-tools": patch
---

FFI (Swift/Kotlin/Go) queries: `{"op": "isNull", "value": false}` now means "is not null", as in TS; it used to mean "is null". `notIn`, `eq: null` and `ne: null` work as in TS. `gt`/`gte`/`lt`/`lte`/`contains` with `null` and `isNotNull: false` are refused.
