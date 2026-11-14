---
"jazz-tools": patch
---

On a server, `db.onMutationError` now receives only rejections of writes made through that `Db` (backend-wide dbs such as `jazz.db`, which all write as the backend, share theirs). Before, every listener on the backend saw every user's rejections, so a handler on one user's `jazz.forReqUser()` db could be told about another user's rejected write. A per-request handler can now forward what it receives to its requester. Rejections Jazz can't trace to the `Db` that made them (writes made while no listener was registered, or in-flight writes settled after a restart) go only to `jazz.db`, never to a per-request db. A listener on `jazz.db` no longer sees rejections of writes made through `forReqUser()` or `withAttribution()` dbs: register on those dbs to handle them.
