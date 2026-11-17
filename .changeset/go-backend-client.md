---
"jazz-tools": patch
---

The Go module (`jazz-go/v2`) connects as a backend client of your main backend's `/jazz`: `jazz.Credential` opens with a machine's OIDC token, `db.BackendClient` says whether it is trusted or scoped, a trusted client acts for a user with `db.ForUser`, and `db.Shutdown(ctx)` uploads queued writes before closing.
