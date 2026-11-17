---
"jazz-tools": patch
---

**Rust: a write refused before it leaves the client carries TypeScript's `code`.** `jazz::tools::JazzClient` now returns `JazzError::WriteRejected { code, reason }` instead of `JazzError::Write(..)` when the core refuses a write synchronously, and `jazz-app-support` returns `Error::WriteRejected`. `code` is TS's, and `reason` is the core's detail. This is the same variant a rejected durable wait returns, so one match handles a refusal wherever it is decided. It covers:

- **writes** (`insert`, `update`, `upsert`, `restore`, `delete` and their transaction forms) whose input or target the core's validation refuses, for example an insert reusing an id already in use: code `write_rejected` (or `permission_denied` for a policy refusal decided synchronously).
- **transactions** (`begin_*`, `commit_transaction`, `rollback_transaction`) that hit a local transaction conflict: code `transaction_conflict`.

Input the Rust client checks itself before calling the core, such as an unknown column or a missing required column, is still `JazzError::Write`.

**Breaking:** code that matched these refusals as `JazzError::Write(message)` still compiles but no longer matches them: match on `WriteRejected { code, .. }` instead.
