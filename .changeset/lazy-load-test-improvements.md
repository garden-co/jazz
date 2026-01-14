---
"cojson": patch
---

Optimized initial CoValue sync, now if there is no content to be synced the sync-server won't load the CoValue in memory only their known state.