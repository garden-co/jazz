---
"jazz-tools": patch
---

Fix updates that could fail to settle after concurrent local writes by stopping redundant merges once only one underlying edit remains.
