---
"jazz-tools": patch
---

New `db.isLocalFirst()` tells whether a `Db` reads and writes through a local store yet: true for a local-first `Db`, and for a thin one once the session has upgraded it in place.
