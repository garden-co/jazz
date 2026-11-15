---
"jazz-tools": patch
---

The Inspector's `explain` now follows inherited clauses one level. For a clause like "the row projectId points to allows read", each row names the parent row it points to and that row's own verdict (whether the user may read it, and by which of the parent table's clauses): `rows[].via` and `inherited` in the answer, and a "↳ projects …: readable (owner = session.user)" line in the Inspector's **Explain why**. Parents in a table an agent token may not see are left out.
