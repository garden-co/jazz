---
"jazz-tools": patch
---

Missing-policy warnings from `jazz-tools validate` and deploys now say what the missing grant denies, that your backend isn't checked when it acts as the system (so a table only your backend writes is fine), and how to declare that on purpose with `.never()` instead of adding an allow rule to silence the warning. The Declarative Permissions page gains a short section on tables only your backend writes.
