---
"jazz-tools": patch
---

Fix a browser tab's write failing with a storage error ("UNIQUE constraint failed") after its worker shared another tab's unconfirmed change to the same row, and keep a tab's own unconfirmed inserts its own while its worker uploads them.
