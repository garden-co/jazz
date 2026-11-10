---
"jazz-tools": patch
---

Keep ordinary replica reads at their transaction snapshot while checking accepted
evidence for the actual owner in the same batch. The opt-in mode cannot select a
delegated session or run on a backend. It refuses accepted evidence after pending
own data is exposed, and retains live owner, claims, read-set and retirement checks.
