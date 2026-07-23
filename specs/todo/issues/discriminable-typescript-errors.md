# Discriminable TypeScript Errors

## What

The TypeScript API currently throws too many plain, message-only `Error`s, so app code cannot reliably distinguish
failures with any reliable mechanism such as `instanceof` or typed `Error.name` values.

## Priority

medium

## Notes

- Use `Error.name` as the stable discriminator. Do not add a separate `code` field.
- Keep names in the existing PascalCase style, e.g. `RecoveryPhraseError`, `PasskeyBackupError`, `FileNotFoundError`,
  and `JazzAnonymousWriteDeniedError`.
- Group errors by the surface area that can throw them. For example, database query/write errors should have their own
  group; recovery-phrase and passkey-backup errors should not appear as possible DB query failures.
- Re-parent existing typed errors, including file-storage, recovery-phrase, passkey-backup, persisted-write-rejection,
  and anonymous-write-denied errors.
- Preserve Rust error structure before it reaches JS. In particular, `RuntimeError::QueryError(String)` should become
  typed enough to retain the underlying query/write failure instead of flattening it to a string.
- `RuntimeError` currently flattens most `QueryError` variants into `QueryError(String)` / `WriteError(String)` via
  `to_string()`. Only `AnonymousWriteDenied { table, operation }` survives as structured data.
- NAPI and WASM CRUD bindings currently wrap failures as message-only errors like `Insert failed: {e}` and
  `Update failed: {e}` instead of preserving typed Rust variants across the boundary.
- `AnonymousWriteDeniedError` is the typed precedent: it has fields, a stable `name`, and a `cause`, but NAPI's
  message-only path makes string parsing the only option. That is exactly what app code cannot rely on, because
  messages are not a formal API, and it is the pattern this issue should remove.
- Do not expose policy denial reasons on client-facing policy errors for now; they may leak sensitive server-side
  details. See [policy-error-reasons](policy-error-reasons.md)
