---
"jazz-tools": patch
---

Fix deletes of rows created under earlier schema versions when permission checks need the row's historical content. Rejected migrated deletes now also leave the row usable for a subsequent update.
