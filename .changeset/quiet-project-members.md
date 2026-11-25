---
"jazz-tools": minor
---

Add `db.encryption.members.add`, `list`, and `remove` with typed space tables, and `tx.encryption.members.add` for transactions. A signed-in client adds the creator to a new space in the same transaction. Set `addCreatorAsMember: false` on the insert or in the client's `encryption` options to opt out.

The first space insert with automatic membership on a thin browser client loads the local-first encryption runtime. Either opt-out keeps plaintext space inserts on the thin client.
