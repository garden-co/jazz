---
"jazz-napi": patch
---

Refuse symlinks, directories and foreign SQLite or RocksDB store files inside
a native database root before SQLite opens it. Existing SQLite files,
recognised regular sidecars and unrelated regular files such as `.DS_Store`
remain supported; refused roots retain their existing contents.
