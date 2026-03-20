---
"jazz-tools": patch
"jazz-wasm": patch
---

Use xxHash-based checksums for `opfs-btree` pages and superblocks to reduce checksum overhead in persistent browser storage.

Existing OPFS stores created by older builds are not checksum-compatible with this change and will need to be recreated after upgrading.
