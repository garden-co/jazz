---
"jazz-tools": patch
---

List encrypted space members with `db.encryption.members.where` and remove an account with `db.encryption.members.remove`. Removal returns the number of visible member rows deleted once the authority accepts the transaction. Call `db.encryption.keys.rotate` to rotate the space key immediately afterwards.
