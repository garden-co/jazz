---
"jazz-tools": patch
---

Cap oversized secondary index keys to a 5 KiB budget so large text values still use the truncate-and-hash encoding without producing OPFS index entries that can overflow B-tree page splits.
