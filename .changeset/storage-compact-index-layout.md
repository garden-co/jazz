---
"jazz-tools": patch
---

Secondary-index entries are now much smaller on disk: each entry is keyed by a small numeric index id plus the index columns written once, stores only the primary-key columns the index lacks, and has an empty value. Per entry, the fk index shrinks from 204 to 49 bytes, `by_seq` from 144 to 40 and `by_global_time` from 126 to 30. Stores that hold rows now declare the `groove.durable-index.v2` storage codec family, so a native, relay or Core store written by alpha.59 or earlier is refused when it is opened, with a typed `UnsupportedStorageCodecs` error. There is no migration: delete the old store (or keep the previous release). Browser IndexedDB stores are refused by the existing manifest check. The server's account registry and catalogue-entry store are unchanged.
