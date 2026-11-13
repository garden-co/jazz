---
"jazz-tools": patch
---

The Inspector can now explain why a user can or can't read each row. `explain` (the Inspector's **Explain why** on a linked query viewed as a user, `jazz-tools inspect explain '<query>' --as <user>`, and the MCP `explain` tool) lists every row the query matches with the backend's authority, whether the user may read it, and which clause of the table's read policy allowed it. Clauses are the parts of the policy's top-level `or`, shown as text such as `owner = session.user`. It used to answer "not supported yet".
