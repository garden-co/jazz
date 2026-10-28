---
"jazz-napi": patch
---

Refuse unrecognised files, directories and symlinks inside a native database
root before SQLite opens it. Existing SQLite files and recognised regular
sidecars remain supported; refused roots retain their existing contents.
