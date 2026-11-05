---
"jazz-tools": patch
---

When a text merge runs past its work budget and the later write stands as sent, the server now logs a warning that names the app, the table, the row and the column, because a concurrent edit has left the current text (history keeps it). Until now this happened silently.
