---
"jazz-wasm": patch
---

Make `StreamingMutation.abort()` reliably settle a push that is already in flight, evicting staged upload state instead of returning early while the storage operation continues.
