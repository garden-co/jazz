---
"jazz-tools": patch
---

Breaking: `session.user` is now the account, and `session.identity` is the exact login (`{ issuer, subject }`). This holds in permissions and on the client session (`useSession()`, `db.getAuthState().session`).

- `{ $createdBy: session.user }` matches rows written through any identity linked to the account, rows imported from alpha.59 included. `{ "$createdBy.identity": session.identity }` matches only the exact login.
- Replace `session.user.account` with `session.user`, and `session.user.identity` with `session.identity`. The old forms throw an error that names the replacement. A mismatched comparison, such as `$createdBy` with `session.identity`, is refused when the schema compiles and names the forms that compare.
- An accountless session (an anonymous reader) has `session.user === null` and matches no author record.
