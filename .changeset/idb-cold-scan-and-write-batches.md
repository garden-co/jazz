---
"jazz-wasm": patch
"jazz-tools": patch
---

Speed up IndexedDB storage. A cold scan in a fresh tab or worker reads one IndexedDB batch per tree level instead of one per page, and large write batches edit pages they already own in place instead of copying them. The page format is unchanged.
