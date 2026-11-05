---
"jazz-tools": patch
"jazz-napi": patch
---

Offline writes waiting to upload are now stored in tables shaped like the rows they write, with one row per written cell holding a plain SQLite value, instead of as encoded sync messages. A future change to the sync protocol no longer strands writes a user made offline before upgrading.
