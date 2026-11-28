---
"jazz-tools": patch
---

A store whose commit seq row (`commit_seq.v1`) can't be trusted now fails to open with an error that names the fix. That happens when the row holds a value that isn't a seq, or a trigger under the row's names has SQL this build didn't write. Deleting that row, or dropping that trigger, is safe: the next open recovers the seq by scanning, and the next write records it again.
