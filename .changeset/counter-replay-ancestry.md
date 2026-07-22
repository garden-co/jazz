---
"jazz-tools": patch
---

Fix counter merge replay over-counting after reconnect and scoped sync delivery.

Scoped visible snapshots now preserve local raw ancestry and update only the
materialised projection when replaying merged values, so concurrent and causal
counter updates converge without double-counting.
