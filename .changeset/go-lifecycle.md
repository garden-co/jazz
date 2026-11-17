---
"jazz-tools": patch
---

The Go module (`jazz-go/v2`) has TS's client lifecycle: `jazz.Mergeable()` transactions (TS's default), `db.Disconnect` / `db.Reconnect`, `db.DeleteClientStorage`, and `db.OnMutationError` for refused writes nothing waited on.
