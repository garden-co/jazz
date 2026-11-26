---
"jazz-tools": minor
---

Add `db.encryption.members.add`, `list`, and `remove` with typed space tables, and `tx.encryption.members.add` for transactions. A signed-in client adds the creator to a new space in the same transaction. Set `addCreatorAsMember: false` on the insert or in the client's `encryption` options to opt out.

The first space insert with automatic membership on a thin browser client loads the local-first encryption runtime. Either opt-out keeps plaintext space inserts on the thin client.

Adding a member is idempotent: adding an account that's already a member of the space, yourself included, returns its existing member row and writes nothing. So an app that still adds the creator after creating the space lists it once. The check reads the client's local data: a client that hasn't synced an existing membership yet adds a second row, and so can two concurrent adds of the same account (in `Promise.all`, say). `remove` deletes every row. Direct inserts into `__e2ee_members` don't check this. Because the check reads first, `db.encryption.members.add` returns a `MemberWrite`: await it for the row, or call `.wait({ tier })` as on an insert's result. `tx.encryption.members.add` returns a promise of the row.
