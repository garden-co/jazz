---
"jazz-tools": patch
---

- Relation policies with a plain literal in a hop condition or relation filter (for example `exists shares(todo = this.id).todoRow(name = 'x')`, or the DSL's `.hop("todoRow", { name: "x" })`) now deploy. `jazz-tools validate` and the dev server already accepted them; publishing refused them.
- React Native: the native relay reports a refused credential with its classified reason (`expired`, `missing`, `invalid`, `disabled` or `revoked`) instead of the server's message, so the client no longer re-reads the server's wording to decide whether to renew.
