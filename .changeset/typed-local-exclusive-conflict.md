---
"jazz-tools": patch
---

Report a local exclusive transaction conflict as a `PersistedWriteRejectedError` with code `transaction_conflict`, instead of a plain `Error`, so apps can handle it with the same `instanceof` check as an authority rejection.
