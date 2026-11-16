---
"jazz-tools": patch
---

**Rust: a rejected write carries TypeScript's `code` and `reason`.** In `jazz-app-support`, a durable write, a transaction, or `wait_for_sync` that the server rejects now fails with `Error::WriteRejected { code, reason, .. }` (like TS's `PersistedWriteRejectedError`). Each `MutationError` also has `code` and `reason` (like TS's `MutationErrorEvent`). The codes are TS's, so a program can match on them: `permission_denied`, `serialization_conflict`, `transaction_conflict`, `client_clock_too_far_ahead`, and `write_rejected` for any other rejection.

**Breaking:**

- `jazz::tools::JazzError` is now `#[non_exhaustive]` and has a new `WriteRejected { code, reason }` variant: a `match` on it needs a wildcard arm.
- A policy or conflict rejection from `JazzClient::wait_for_transaction` or `wait_for_sync` is now `JazzError::WriteRejected` (in `jazz-app-support`, `Error::WriteRejected`) instead of `JazzError::Sync(..)` / `JazzError::Write(..)`, and its message reads `Write rejected (permission_denied): …`. Code that matched the old variant or message, such as `Sync(message) if message.ends_with("authorization_denied")`, still compiles but no longer matches: match on `WriteRejected { code, .. }` instead.
- `jazz::tools::MutationError` has new public fields `code` and `reason` and is now `#[non_exhaustive]`: it can no longer be built with a struct literal or destructured without `..` outside `jazz`.
