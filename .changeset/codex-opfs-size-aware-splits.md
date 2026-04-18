---
"jazz-tools": patch
---

Fix OPFS B-tree page splitting for large index keys by choosing split points based on encoded page size instead of entry count. This prevents synced inserts with many near-threshold JSON index values from failing with leaf or internal split fit errors.
