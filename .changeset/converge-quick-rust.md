---
"jazz-tools": patch
---

- Relation policies with a plain literal in a hop condition or relation filter (for example `exists shares(todo = this.id).todoRow(name = 'x')`, or the DSL's `.hop("todoRow", { name: "x" })`) now deploy. `jazz-tools validate` and the dev server already accepted them; publishing refused them.
- A plain literal in a relation policy now compares as the column's type: UUID-shaped text compared with a text column is text, and UUID text in any spelling (hyphenated, plain hex or braced) compared with a UUID column is a UUID. Before, policy evaluation in SQL decided by the text's shape, so on a mismatch `=` matched no row and `!=` matched every row.
- React Native: the native relay reports a refused credential with its classified reason (`expired`, `missing`, `invalid`, `disabled` or `revoked`) instead of the server's message, so the client no longer re-reads the server's wording to decide whether to renew.
