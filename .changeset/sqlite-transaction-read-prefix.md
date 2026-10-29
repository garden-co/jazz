---
"jazz-tools": patch
---

Keep a pending transaction read at the writes staged when it began, even when later writes arrive before the authority replies. The final commit still includes all staged writes.
