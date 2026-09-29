---
"jazz-tools": patch
---

Exclusive transactions that read through a join, an include or a related list now record only the related rows they could have used. They no longer download every related row the reader can see, and a write to an unrelated row in the related table no longer makes them conflict.
