---
"jazz-tools": patch
---

Replace `WriteResult` and `WriteHandle` with one `MutationResult<T>` API. All mutations now expose `value`, `batchId`, and `wait`; mutations that do not return a value use `MutationResult<void>`.
