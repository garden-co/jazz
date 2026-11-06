---
"jazz-tools": patch
---

After the background upgrade to the local-first client, local-first reads show the rows the app wrote while it was still on the thin client. Before, `db.all(table, { tier: "local-first" })` could miss the app's own write for a few seconds, even after the write's global wait resolved. New `db.isLocalFirst()` tells whether the session has upgraded yet. Slow-query reports only suggest an index when the slow run scanned or sorted at least 1,000 rows.
