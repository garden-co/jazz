---
"jazz-tools": patch
---

Thin clients now report a core error's stable snake_case code (`transaction_conflict`, `storage`, ...) as `error.code`, the same code a local-first client reports. Before, a thin client got the Rust variant name (`TransactionConflict`), so code that retried on `transaction_conflict` missed a thin client's serializable conflict.
