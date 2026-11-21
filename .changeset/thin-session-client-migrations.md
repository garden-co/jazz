---
"jazz-tools": patch
---

A thin client's transactions served in the app's backend no longer fail with `store_opened_without_migrations` when the app ships migrations: the backend opens each thin session's replica with the app's migrations.
