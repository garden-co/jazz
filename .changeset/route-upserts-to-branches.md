---
"jazz-tools": patch
"jazz-wasm": patch
"jazz-napi": patch
---

Route ordinary and mergeable-transaction upserts through their complete head-over-base branch view. Head-local rows are merged, inherited rows (including verified indirect large values) are copied into the head, and absent rows are inserted there consistently across native and WASM runtimes. Committed tombstones remain rejected; a later upsert in the same mergeable transaction supersedes its pending delete so replacement content is visible. Session transactions can upsert branch rows staged earlier in that transaction.

Low-level JavaScript callers may use `{ head, base? }` or the legacy `{ branch }` alias for `{ head: branch }`; mixing the two forms is rejected instead of silently selecting the root target. Rust callers must migrate `UpsertOptions::target` from `ExactWriteTarget` to `WriteTarget` when constructing that field.
