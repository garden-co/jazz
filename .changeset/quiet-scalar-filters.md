---
"jazz-tools": patch
---

Classify typed where values from their declared column types so Date, Uint8Array, JSON-object, and array equality filters lower as a single `eq` condition while operator maps retain their existing validation.
