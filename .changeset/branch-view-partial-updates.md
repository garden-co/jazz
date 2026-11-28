---
"jazz-tools": patch
---

Support `applyDiffs` and `editedFrom` in branch views. An edit of a row a draft inherits applies to the base's value (a frozen base's value at its cut) and copies the row into the draft. Fix a text-merge data loss: when two writers made the first edit of the same inherited row in a draft at once, the authority kept only the later write. Their edits now merge, as counter and set columns already did. React Native and the thin client still refuse partial-value updates in branch views, now with an explicit "not supported yet" error.
