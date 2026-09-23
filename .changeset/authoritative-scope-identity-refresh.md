---
"jazz-tools": patch
---

Fix subscriptions prepared before the first server catalogue arrives retaining temporary table identities. After adopting the server's identities, Jazz now refreshes the affected sync metadata while preserving pending writes and existing local query subscriptions, preventing intermittent permission-scoped reads from being rejected or remaining stuck during startup.
