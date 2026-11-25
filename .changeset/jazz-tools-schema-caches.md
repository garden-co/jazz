---
"jazz-tools": patch
---

React Native and other clients compute per-schema data (relation analysis, logical storage columns, the sorted tables JSON) once per schema object instead of once per query or decoded row. The serialized schema is byte-identical.
