---
"jazz-tools": patch
---

Compressing a small sync message no longer allocates a buffer the size of a full frame, and incoming compressed messages decode straight into their reassembly buffer instead of through temporary copies. The wire format is unchanged.
