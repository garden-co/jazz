---
"jazz-tools": patch
---

A reconnecting client no longer sends frames produced for the dropped connection on the new one, which could fail the reconnected link with a channel credit sequence mismatch.
