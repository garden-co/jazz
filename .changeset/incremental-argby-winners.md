---
"jazz-tools": patch
---

Maintain ordered ArgMin/ArgMax candidates incrementally instead of rebuilding whole groups on each update, reducing repeated-update costs for deep row histories while preserving deterministic ties and retractions.
