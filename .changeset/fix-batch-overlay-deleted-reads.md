---
"jazz-tools": patch
---

Keep transaction and direct-batch writes isolated from ordinary indexed queries until commit, while still letting batch-scoped reads see their own staged inserts, updates, and deletes.
