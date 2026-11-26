---
"jazz-tools": minor
---

Add `db.encryption.members.add`, `list`, and `remove` with typed space tables, and `tx.encryption.members.add` for transactions. A signed-in client adds the creator to a new space in the same transaction. Set `addCreatorAsMember: false` on the insert or in the client's `encryption` options to opt out.

The first space insert with automatic membership on a thin browser client loads the local-first encryption runtime. Either opt-out keeps plaintext space inserts on the thin client.

Inserting your own membership of a space you're already a member of is refused ("already a member"), so an app that still adds the creator after creating the space doesn't list it twice. Remove that insert, or opt out of automatic membership. Adding other accounts is unchanged.
