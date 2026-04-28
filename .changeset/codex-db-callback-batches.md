---
"jazz-tools": patch
---

Replace the typed `Db` table-seeded transaction and batch APIs with callback-scoped helpers that automatically commit and return a waitable write handle.
