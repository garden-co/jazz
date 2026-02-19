---
"cojson": patch
---

Throw immediately when calling `.create()` on a group where the current user does not have write permissions, instead of silently producing empty data.
