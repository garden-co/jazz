---
"jazz-tools": patch
---

Breaking: `session.user` is now the account, and `session.identity` is the exact login (`{ issuer, subject }`). This holds in permissions and on the client session (`useSession()`, `useAuthState()`, `db.getAuthState().session`).

- `{ $createdBy: session.user }` matches rows written through any identity linked to the account, including alpha.59 rows whose author was imported with an account. `{ "$createdBy.identity": session.identity }` matches only the exact login.
- Replace `session.user.account` with `session.user`, and `session.user.identity` with `session.identity`. The old forms throw an error that names the replacement. A mismatched comparison, such as `$createdBy` with `session.identity`, is refused when the schema compiles and names the forms that compare.
- An accountless session (an anonymous reader) has `session.user === null`, so `{ $createdBy: session.user }` matches no row for it.
- `{ $createdBy: session.user }` used to match only the exact login that wrote the row. It now matches the whole account; if you relied on the exact login, switch to `{ "$createdBy.identity": session.identity }`.
- Redeploy your permissions together with the upgrade. A stored permissions bundle that still uses `session.user.account` or `session.user.identity` no longer compiles on the new server, so reads and writes under it fail until the new permissions are published.
