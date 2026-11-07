---
"jazz-tools": patch
---

Thin clients (the default) can write text edits again: `db.update(..., { editedFrom })` and `applyDiffs` splices on large values no longer fail with `NotObserved: large-value cell is not observed`. The server read the cell under its own schema instead of the session's published permissions, so every row looked unreadable.
