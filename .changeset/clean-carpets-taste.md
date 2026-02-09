---
"jazz-tools": patch
---

Reverted the Expo SQLite adapter to use non-exclusive transactions, to fix the "database is locked" error when read queries are executed in the middle of a transaction.
