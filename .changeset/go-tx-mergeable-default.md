---
"jazz-tools": patch
---

Go: `db.Tx(ctx, fn)` is now a mergeable transaction by default, as TypeScript's `db.transaction` (its writes merge with concurrent ones, and its commit never fails with `jazz.ErrConflict`). Pass `jazz.Serializable()` for a serializable transaction, as `db.serializableTransaction`; `jazz.Mergeable()` stays, as an explicit no-op.
