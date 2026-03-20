---
"jazz-tools": patch
---

Remove local write-time foreign-key existence checks so inserts and updates no longer fail just because a referenced row has not been synced into the active query set yet.
