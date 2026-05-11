---
"jazz-tools": patch
---

Fix `s.timestamp()` row output inference so timestamp columns are typed as `Date` instead of `Date | number`.

Numeric timestamp defaults remain accepted, but inserted and queried rows now match the runtime shape and infer timestamp values as JavaScript `Date` objects.
