---
"jazz-rn": patch
---

The SQLite storage that React Native uses through its native relay compiles each statement once per connection instead of on every call, and scans read 256-row pages lazily instead of loading the whole range into memory. Write batches bind their keys and values without copying them first. The on-disk format and results are unchanged.
