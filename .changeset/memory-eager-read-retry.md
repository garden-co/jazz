---
"jazz-tools": patch
---

In-memory databases now honour their storage's eager read retry setting, which lets a read that yields once be re-polled in the same turn.
