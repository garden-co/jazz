---
"jazz-tools": patch
---

Make appending to or editing a large text or bytes value cost proportional to the edit rather than the whole value, by validating only the new part of a locally derived value and no longer rebuilding the value to find the row's current version.
