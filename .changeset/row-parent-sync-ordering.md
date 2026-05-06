---
"jazz-tools": patch
---

Queue unsent parent row batches before child row batches when syncing to servers. This keeps server-side permission checks from evaluating updates before their prior row content has arrived.
