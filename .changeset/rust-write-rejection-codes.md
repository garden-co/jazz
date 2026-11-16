---
"jazz-tools": patch
---

**Rust: a rejected write carries TypeScript's `code` and `reason`.** In `jazz-app-support`, a durable write, a transaction, or `wait_for_sync` that the server rejects now fails with `Error::WriteRejected { code, reason, .. }` (like TS's `PersistedWriteRejectedError`). Each `MutationError` also has `code` and `reason` (like TS's `MutationErrorEvent`). The codes are TS's, so a program can match on them: `permission_denied`, `serialization_conflict`, `transaction_conflict`, `client_clock_too_far_ahead`, and `write_rejected` for any other rejection. `jazz::tools::JazzError` has the matching `WriteRejected` variant. `MutationError` is now `#[non_exhaustive]`.
