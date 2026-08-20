---
"jazz-tools": patch
---

Breaking: replace `WriteResult` and `WriteHandle` with one `MutationResult<T>` API. All mutations
now expose `value`, `transactionId`, and `wait`; mutations that do not return a value use
`MutationResult<void>`.
