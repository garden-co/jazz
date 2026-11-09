---
"jazz-tools": patch
---

Remove the `policy.<table>.managedByCreator()` and `isCreator` permission shorthands. Write the creator condition explicitly as `{ $createdBy: session.user }` on each rule instead, for example `policy.todos.allowRead.where({ $createdBy: session.user })`.
