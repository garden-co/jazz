---
"jazz-tools": patch
---

Reclaim obsolete IndexedDB pages during writes under exclusive ownership, and safely retire browser runtime storage handles during schema changes and reconnects. Existing unused pages are not automatically collected.
