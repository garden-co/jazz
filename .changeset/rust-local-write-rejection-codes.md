---
"jazz-tools": patch
---

**Rust: a write refused before it leaves the client carries TypeScript's `code`.** When a policy (or another rejection the client decides itself, such as a taken id) refuses an `insert`, `update`, `upsert`, `restore` or `delete` synchronously, `jazz::tools::JazzClient` now returns `JazzError::WriteRejected { code, reason }` instead of `JazzError::Write(..)`, and `jazz-app-support` returns `Error::WriteRejected`. `code` is TS's (`permission_denied`, `write_rejected`, …); `reason` is the core's detail, e.g. `policy denied INSERT on table notes`. This is the same variant a rejected durable wait returns, so one match handles a refusal wherever it is decided.

**Breaking:** code that matched these refusals as `JazzError::Write(message)` still compiles but no longer matches them: match on `WriteRejected { code, .. }` instead.
