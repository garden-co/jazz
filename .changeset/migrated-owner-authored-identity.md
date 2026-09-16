---
"jazz-tools": patch
---

Preserve authored transaction identity when a persistent owner relays reads across schema migrations. Old and new schema reads retain original row bytes and authored metadata, and renamed-table witnesses retain their exact branch.
