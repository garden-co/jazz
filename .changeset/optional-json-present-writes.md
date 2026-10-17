---
"jazz-tools": patch
---

Writing a value to an optional JSON column (`s.json().optional()`) from TypeScript now works. Previously every insert or update that set such a column failed with `value does not match type Internal(InternalValueType(StoredScalar(Json)))`. Writing `null` to it is still tracked in #2733.
