---
"jazz-tools": patch
---

Read native encryption documents in one private-store callback at startup. Swift and Kotlin private stores must implement `loadAll` with an incarnation-checked snapshot. Go stores can opt in with `LoadAll`. Jazz continues to authenticate each record and preserve its saved revision.
