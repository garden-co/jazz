---
"jazz-wasm": patch
"jazz-tools": patch
---

Fix intermittent browser startup failures with `IDBTree page … exceeds the configured page size` (PageTooLarge). When an IndexedDB B-tree page holding entries of very different sizes overflowed, it was split by entry count, which could leave one half still too large. Pages now split at a byte-balanced boundary where both halves fit. The IndexedDB storage format is unchanged.
