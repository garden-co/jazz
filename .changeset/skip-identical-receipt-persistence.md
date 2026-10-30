---
"jazz-tools": patch
---

Skip rewriting storage when a sync server re-delivers an already-applied acceptance receipt for a complete mergeable transaction, making reconnects with many settled writes cheaper.
