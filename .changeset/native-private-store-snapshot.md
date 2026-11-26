---
"jazz-tools": minor
---

**Breaking for Swift and Kotlin custom private stores.** Implement `loadAll` to return the documents in one incarnation-checked snapshot. Jazz uses this callback at startup. Go stores can opt in with `LoadAll`. Jazz continues to authenticate each record and preserve its saved revision.
